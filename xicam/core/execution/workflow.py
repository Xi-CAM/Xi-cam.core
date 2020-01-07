from xicam.plugins import ProcessingPlugin, OperationPlugin
from typing import Callable, List, Union
from collections import OrderedDict
from xicam.core import msg, execution
from xicam.core.threads import QThreadFuture, QThreadFutureIterator

# TODO: add debug flag that checks mutations by hashing inputs


class WorkflowProcess:
    def __init__(self, node, named_args, islocal=False):
        self.node = node
        self.named_args = named_args
        self.islocal = islocal
        self.queues_in = {}
        self.queues_out = {}

        self.node.__internal_data__ = self

    # args = [{'name':value}]

    def __call__(self, *args):
        node_args = {}
        for arg, (input_name, sender_operation_name) in zip(args, self.named_args.items()):
            node_args[input_name] = arg[sender_operation_name]

        result_keys = self.node.output_names
        result_values = self.node(**node_args)
        if not isinstance(result_values, tuple): result_values = (result_values,)

        return dict(zip(result_keys, result_values))

    def __repr__(self):
        return self.node.__class__.__name__


class Workflow(object):
    def __init__(self, name="", operations=None):
        self._operations = []  # type: List[OperationPlugin]
        self._observers = set()
        if name:
            self.name = name

        if operations:
            self._operations.extend(operations)
        self.staged = False

        self.lastresult = []

    def findEndTasks(self):
        """
        find tasks at the end of the graph and work up
        check inputs and remove dependency nodes, what is left is unique ones
        """

        dependent_tasks = set()

        for operation in self.operations:
            for weak_ref_operation, _, __ in operation._inbound_links:
                dependent_tasks.add(weak_ref_operation())

        end_tasks = set(self.operations) - dependent_tasks

        msg.logMessage("End tasks:", *[task.name for task in end_tasks], msg.DEBUG)
        return end_tasks

    def generateGraph(self):
        """
        Recursive function that adds
        :param dsk:
        :param q:
        :param operation:
        :param mapped_node:
        :return:
        """

        dask_graph = {}

        for operation in self.operations:
            links = {}
            dependent_ids = []
            for weak_op_ref, output_name, input_name in operation._inbound_links:
                links[input_name] = output_name
                dependent_ids.append(weak_op_ref().id)
            node = WorkflowProcess(operation, links)

            dask_graph[operation.id] = (node, *dependent_ids)

        return dask_graph

    def convertGraph(self):
        """
        process from end tasks and into all dependent ones
        """

        for (i, node) in enumerate(self.operations):
            node.id = str(i)

        end_tasks = self.findEndTasks()

        dask_graph = self.generateGraph()
        end_task_ids = [i.id for i in end_tasks]

        return dask_graph, end_task_ids

    def addOperation(self, operation: Union[OperationPlugin, callable], autoconnectall: bool = False):
        """
        Adds a Operation as a child.
        Parameters
        ----------
        operation:    OperationPlugin
            Operation to add
        autowireup: bool
            If True, connects Outputs of the previously added Operation to the Inputs of operation, matching names and types
        """
        if not isinstance(operation, OperationPlugin) and callable(operation):
            operation = OperationPlugin(operation)

        self._operations.append(operation)
        operation._workflow = self
        if autoconnectall:
            self.autoConnectAll()
        self.update()
        return operation

    def insertOperation(self, index: int, operation: OperationPlugin, autoconnectall: bool = False):
        self._operations.insert(index, operation)
        operation._workflow = self
        self.update()
        if autoconnectall:
            self.autoConnectAll()

    def removeOperation(self, operation: OperationPlugin = None, index=None, autoconnectall=False):
        if not operation:
            operation = self._operations[index]
        self._operations.remove(operation)
        operation.detach()
        operation._workflow = None
        if autoconnectall:
            self.autoConnectAll()
        self.update()

    def autoConnectAll(self):
        self.clearConnections()

        # for each operation
        for inputoperation in self.operations:

            # for each input of given operation
            for input in inputoperation.inputs.values():
                bestmatch = None
                matchness = 0
                # Parse backwards from the given operation, looking for matching outputs
                for outputoperation in reversed(self.operations[: self.operations.index(inputoperation)]):
                    # check each output
                    for output in outputoperation.outputs.values():
                        # if matching name
                        if output.name == input.name:
                            # if a name match hasn't been found
                            if matchness < 1:
                                bestmatch = output
                                matchness = 1
                                # if a name+type match hasn't been found
                                if output.type == input.type:
                                    if matchness < 2:
                                        bestmatch = output
                                        matchness = 2
                if bestmatch:
                    bestmatch.connect(input)
                    msg.logMessage(
                        f"connected {bestmatch.parent.__class__.__name__}:{bestmatch.name} to {input.parent.__class__.__name__}:{input.name}",
                        level=msg.DEBUG,
                    )

                    # # for each output of given process
                    # for output in process.outputs.values():
                    #     bestmatch = None
                    #     matchness = 0
                    #     # Parse backwards from the given process, looking for matching outputs
                    #     for process in self.processes[self.processes.index(process) + 1:]:
                    #         # check each output
                    #         for input in process.inputs.values():
                    #             # if matching name
                    #             if output.name == input.name:
                    #                 # if a name match hasn't been found
                    #                 if matchness < 1:
                    #                     bestmatch = input
                    #                     matchness = 1
                    #                     # if a name+type match hasn't been found
                    #                     if output.type == input.type:
                    #                         if matchness < 2:
                    #                             bestmatch = input
                    #                             matchness = 2
                    #     if bestmatch:
                    #         output.connect(bestmatch)

    def clearConnections(self):
        for operation in self.operations:
            operation.clearConnections()

    def toggleDisableOperation(self, operation, autoconnectall=False):
        operation.disabled = not operation.disabled
        operation.clearConnections()
        if autoconnectall:
            self.autoConnectAll()
        self.update()

    @property
    def operations(self) -> List[OperationPlugin]:
        return [operation for operation in self._operations if not operation.disabled]

    @operations.setter
    def operations(self, operations):
        for operation in self._operations:
            operation._workflow = None

        self._operations = operations
        for operation in operations:
            operation._workflow = self
        self.update()

    def stage(self, connection):
        """
        Stages required data resources to the compute resource. Connection will be a Connection object (WIP) keeping a
        connection to a compute resource, include connection.hostname, connection.username...

        Returns
        -------
        QThreadFuture
            A concurrent.futures-like qthread to monitor status. Returns True if successful
        """
        self.staged = True
        # TODO: Operations invalidate parent workflow staged attribute if data resources are modified, but not parameters
        # TODO: check if data is accessible from compute resource; if not -> copy data to compute resource
        # TODO: use cam-link to mirror installation of plugin packages

    def execute(
        self,
            executor=None,
        connection=None,
        callback_slot=None,
        finished_slot=None,
        except_slot=None,
        default_exhandle=True,
        lock=None,
        fill_kwargs=True,
        threadkey=None,
        **kwargs,
    ):
        """
        Execute this workflow on the specified host. Connection will be a Connection object (WIP) keeping a connection
        to a compute resource, include connection.hostname, connection.username...

        Returns
        -------
        QThreadFuture
            A concurrent.futures-like qthread to monitor status. The future's callback_slot receives the result.

        """
        if not self.staged:
            self.stage(connection)

        if fill_kwargs:
            self.fillKwargs(**kwargs)

        if executor is None:
            executor = execution.executor

        future = QThreadFuture(
            executor.execute,
            self,
            callback_slot=callback_slot,
            finished_slot=finished_slot,
            default_exhandle=default_exhandle,
            lock=lock,
            threadkey=threadkey,
        )
        future.start()
        return future

    def execute_synchronous(self, executor, connection=None, fill_kwargs=True, **kwargs):
        if not self.staged:
            self.stage(connection)

        if fill_kwargs:
            self.fillKwargs(**kwargs)

        if executor is None:
            executor = execution.executor

        return executor.execute(self)


    def execute_all(
        self,
        connection,
            executor=None,
        callback_slot=None,
        finished_slot=None,
        except_slot=None,
        default_exhandle=True,
        lock=None,
        fill_kwargs=True,
        threadkey=None,
        **kwargs,
    ):
        """
        Execute this workflow on the specified host. Connection will be a Connection object (WIP) keeping a connection
        to a compute resource, include connection.hostname, connection.username...

        Each kwargs is expected to be an iterable of the same length; these values will be iterated over, zipped, and
        executed through the workflow.

        Returns
        -------
        QThreadFuture
            A concurrent.futures-like qthread to monitor status. The future's callback_slot receives the result.

        """
        if not self.staged:
            self.stage(connection)

        if executor is None:
            executor = execution.executor

        def executeiterator(workflow):
            for kwargvalues in zip(*kwargs.values()):
                zipkwargs = dict(zip(kwargs.keys(), kwargvalues))
                if fill_kwargs:
                    self.fillKwargs(**zipkwargs)
                yield (executor.execute)(workflow)

        future = QThreadFutureIterator(
            executeiterator,
            self,
            callback_slot=callback_slot,
            finished_slot=finished_slot,
            default_exhandle=default_exhandle,
            lock=lock,
            threadkey=threadkey,
        )
        future.start()
        return future

    def fillKwargs(self, **kwargs):
        """
        Fills in all empty inputs with names matching keys in kwargs.
        """
        for operation in self.operations:
            operation.filled_values = kwargs

    def validate(self):
        """
        Validate all of:\
        - All required inputs are satisfied.
        - Connection is active.
        - ?

        Returns
        -------
        bool
            True if workflow is valid.

        """
        # TODO: add validation
        return True

    def attach(self, observer: Callable):
        self._observers.add(observer)

    def detach(self, observer: Callable):
        if observer in self._observers:
            self._observers.remove(observer)

    def update(self):
        for observer in self._observers:
            observer()

    @property
    def hints(self):
        hints = []
        for operation in self._operations:
            hints.extend(operation.hints)
        return hints

    def visualize(self, canvas, **canvases):
        canvasinstances = {name: canvas() if callable(canvas) else canvas for name, canvas in canvases.items()}
        for operation in self._operations:
            for hint in operation.hints:
                hint.visualize(canvas)
