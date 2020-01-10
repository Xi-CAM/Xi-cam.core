from xicam.plugins import ProcessingPlugin, OperationPlugin
from typing import Callable, List, Union, Tuple
from collections import defaultdict
from xicam.core import msg, execution
from xicam.core.threads import QThreadFuture, QThreadFutureIterator
from weakref import ref


# TODO: add debug flag that checks mutations by hashing inputs

class Graph(object):
    def __init__(self):
        self._operations = []
        self._inbound_links = defaultdict(lambda: defaultdict(lambda: []))
        self._outbound_links = defaultdict(lambda: defaultdict(lambda: []))

    def add_operation(self, operation: OperationPlugin):
        self.add_operations([operation])

    def add_operations(self, operations: List[OperationPlugin]):
        for operation in operations:
            self.insert_operation(len(self._operations) + 1, operation)

    def insert_operation(self, index: int, operation: OperationPlugin):
        self._operations.insert(index, operation)
        operation._workflow = ref(self)

        self.notify()

    def add_link(self, source, dest, source_param, dest_param):
        if source_param not in source.output_names:
            raise NameError(
                f"An output named \"{source_param}\" could not be found in the sender operation, {source.name}")
        elif dest_param not in dest.input_names:
            raise NameError(
                f"An input named \"{dest_param}\" could not be found in the receiver operation, {dest.name}")
        self._inbound_links[dest][source].append((source_param, dest_param))
        self._outbound_links[source][dest].append((source_param, dest_param))

        self.notify()

    def remove_link(self, source, dest, source_param, dest_param):
        self._inbound_links[dest][source].remove((source_param, dest_param))
        self._outbound_links[source][dest].remove((source_param, dest_param))

        self.notify()

    def get_inbound_links(self, operation):
        return self._inbound_links[operation]

    def get_outbound_links(self, operation):
        return self._outbound_links[operation]

    def clear_operation_links(self, operation):
        for links in self.get_inbound_links(operation):
            del links
        for links in self.get_outbound_links(operation):
            del links

        self.notify()

    def clear_links(self):
        self._inbound_links.clear()
        self._outbound_links.clear()

        self.notify()

    def clear_operations(self):
        self._operations.clear()
        self._inbound_links.clear()
        self._outbound_links.clear()

        self.notify()

    def remove_operation(self, operation, remove_orphan_links=True):
        self._operations.remove(operation)

        if remove_orphan_links:
            self.clear_operation_links(operation)
        else:
            return self.get_inbound_links(operation), self.get_outbound_links(operation)

        self.notify()

    def _end_operations(self):
        """
        find tasks at the end of the graph and work up
        check inputs and remove dependency nodes, what is left is unique ones
        """

        end_tasks = set(self.operations) - self._outbound_links.keys()

        msg.logMessage("End tasks:", *[task.name for task in end_tasks], msg.DEBUG)
        return end_tasks

    def _dask_graph(self):
        dask_graph = {}

        for operation in self.operations:
            links = {}
            dependent_ids = []
            for dep_operation, inbound_links in self._inbound_links[operation].items():
                for (source_param, dest_param) in inbound_links:
                    links[dest_param] = source_param
                dependent_ids.append(dep_operation.id)

            node = _OperationWrapper(operation, links)
            dask_graph[operation.id] = (node, *dependent_ids)

        return dask_graph

    def as_dask_graph(self):
        """
        process from end tasks and into all dependent ones
        """

        for (i, node) in enumerate(self.operations):
            node.id = str(i)

        end_tasks = self._end_operations()

        dask_graph = self._dask_graph()
        end_task_ids = [i.id for i in end_tasks]

        return dask_graph, end_task_ids

    @property
    def operations(self):
        return self._operations

    def links(self):
        return [(src, dest, src_param, dest_param) for dest, links in self._outbound_links.items() for
                src, (src_param, dest_param) in links.items()]

    def operation_links(self, operation):
        return [(operation, dest, src_param, dest_param) for dest, (src_param, dest_param) in
                self._outbound_links[operation]]

    def set_disabled(self, operation: OperationPlugin, value: bool = True, remove_orphan_links=True):
        operation.disabled = value
        orphaned_links = []
        if value:
            if remove_orphan_links:
                self.clear_operation_links(operation)
            else:
                orphaned_links = self.operation_links(operation)

        self.notify()
        return orphaned_links

    def toggle_disabled(self, operation: OperationPlugin, remove_orphan_links=True):
        return self.set_disabled(operation, not operation.disabled, remove_orphan_links)

    def auto_connect_all(self):
        self.clear_links()

        # for each operation
        for input_operation in self.operations:

            # for each input of given operation
            for input_name in input_operation.input_names:
                bestmatch = None
                matchness = 0
                # Parse backwards from the given operation, looking for matching outputs
                for output_operation in reversed(self.operations[: self.operations.index(input_operation)]):
                    # check each output
                    for output_name in output_operation.output_names:
                        # if matching name
                        if output_name == input_name:
                            # if a name match hasn't been found
                            if matchness < 1:
                                bestmatch = output_operation, output_name
                                matchness = 1
                                # if a name+type match hasn't been found
                                if output_operation.output_types[output_name] == input_operation.input_types[
                                    input_name]:
                                    if matchness < 2:
                                        bestmatch = output_operation, output_name
                                        matchness = 2
                if bestmatch:
                    self.add_link(bestmatch[0], input_operation, bestmatch[1], input_name)
                    msg.logMessage(
                        f"connected {bestmatch[0].name}:{bestmatch[1]} to {input_operation.name}:{input_name}",
                        level=msg.DEBUG,
                    )

    def notify(self, *args, **kwargs):
        ...


class _OperationWrapper:
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


class Workflow(Graph):
    def __init__(self, name="", operations=None):
        super(Workflow, self).__init__()
        # self._operations = []  # type: List[OperationPlugin]
        self._observers = set()
        # self._links = []  # type: List[Tuple[ref, str, ref, str]]
        if name:
            self.name = name

        if operations:
            # self._operations.extend(operations)
            self.add_operations(operations)
        self.staged = False

        self.lastresult = []

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

    def notify(self):
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
