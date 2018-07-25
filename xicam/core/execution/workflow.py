from xicam.plugins import ProcessingPlugin, Input, Output, InOut, Var
from typing import Callable, List
from .camlinkexecutor import CamLinkExecutor
from .localexecutor import LocalExecutor
from collections import OrderedDict
from xicam.core import msg, execution
from xicam.gui.threads import QThreadFuture, QThreadFutureIterator


# TODO: add debug flag that checks mutations by hashing inputs

class WorkflowProcess():
    def __init__(self, node, named_args, islocal=False):
        self.node = node
        self.named_args = named_args
        self.islocal = islocal

        self.node.__internal_data__ = self

        self.queue = None

    def __call__(self, *args):
        import gc
        assigned = []
        if args is not None and len(args) > 0:
            for i in range(len(args)):
                # self.node.inputs[self.named_args[i]].value = args[i][0].value
                for key in args[i].keys():
                    if key in self.named_args:
                        # msg.logMessage(
                        #     f'Setting input {self.node.__class__.__name__}:{self.named_args[key]} to output {args[i][key].value}',
                        #     level=msg.DEBUG)
                        self.node.inputs[self.named_args[key]].value = args[i][key].value
                        assigned.append(key)

        # print("ASSIGNED:", assigned, self.node, self.node.inputs)

        # if self.__repr__() == "Normalize_REG":
        #    print(args[0]["tomo"].value, args[0]["darks"].value, args[0]["flats"].value)
        #    print(self.node.inputs["tomo"].value, self.node.inputs["darks"].value, self.node.inputs["flats"].value)

        print(self.node, "action")
        self.node.evaluate()
        print(self.node, "finished action")
        # gc.collect()

        return self.node.outputs

    def __repr__(self):
        return self.node.__class__.__name__ + str("_REG")


class WorkflowExecuteProcess():
    def __init__(self, i, node, graph, start_tasks, max_length):
        self.priority = i
        self.node = node
        self.graph = graph
        self.start_tasks = start_tasks
        self.max_length = max_length

        self.node.__internal_data__ = self

    def __call__(self, args):
        import gc

        from distributed import worker_client, get_client, secede, rejoin
        from distributed import fire_and_forget

        import distributed
        client = get_client()  # type: distribute.client

        wf = WorkflowProcess(self.node, None)
        # res = client.submit(wf)

        # out = res.result()
        # res2 = client.submit(self.graph["0"][0])
        # res3 = client.submit(self.graph["1"][0], [res2, ] )
        # res4 = client.submit(self.graph["2"][0], [res2, ] )
        # res5 = client.submit(self.graph["3"][0], [res4, res] )
        # res6 = client.submit(self.graph["4"][0], [res5, ] )
        # res7 = client.submit(self.graph["5"][0], [res6, ] )

        # out = res2.result()

        # print(out)

        # client.cancel(res)
        # client.cancel(res2)
        # client.cancel(res3)
        # client.cancel(res4)
        # client.cancel(res5)
        # client.cancel(res6)
        # client.cancel(res7)

        # del res, res2, res3, res4, res5, res6, res7

        # out = res.result()
        # self.node.evaluate()
        # out = self.node.outputs

        # gc.collect()
        return out

    def __repr__(self):
        return self.node.__class__.__name__ + str("_REG")


class WorkflowStreamProcess(object):
    def __init__(self, node, named_args, graph, start_tasks):
        self.node = node
        self.named_args = named_args
        self.graph = graph
        self.start_tasks = start_tasks

        self.node.__internal_data__ = self

        self.queue = None

    # @classmethod
    # def emit(**args):
    #    pass

    def flatten(self, seq, container=list):
        if isinstance(seq, str):
            yield seq
        else:
            for item in seq:
                if isinstance(item, container):
                    for item2 in flatten(item, container=container):
                        yield item2
                else:
                    yield item

    def __call__(self, *args, **kwargs):
        import gc
        # self.queue.put(self.node.len())

        from distributed import get_client, secede, rejoin
        from dask.distributed import fire_and_forget

        import cloudpickle

        client = get_client()

        tasks = []

        import time

        for i, result in enumerate(self.node.emit()):
            print(i, result)
            graph = cloudpickle.dumps(self.graph)
            graph = cloudpickle.loads(graph)

            node = graph["6"][0].node

            for input in result:
                node.inputs[input[0]].value = input[1]

            # print("NODE", node, graph, result)
            # node.evaluate()
            # print("OUT:", node.outputs)

            # wf = WorkflowExecuteProcess(i, node, graph, self.start_tasks, self.node.len())
            wf = WorkflowProcess(node, None)
            res = client.submit(wf)
            res2 = client.submit(self.graph["0"][0], res)
            # res3 = client.submit(self.graph["1"][0], [res2, ])
            res4 = client.submit(self.graph["2"][0], res2)
            res5 = client.submit(self.graph["3"][0], res4, res)
            res6 = client.submit(self.graph["4"][0], res5)
            res7 = client.submit(self.graph["5"][0], res6)
            time.sleep(.1)
            # fire_and_forget(res)

            for i, task in enumerate(tasks):
                if task.done():
                    del tasks[i]

            tasks.append(res)
            tasks.append(res2)
            tasks.append(res4)
            tasks.append(res5)
            tasks.append(res6)
            tasks.append(res7)

            # ret = res.result()
            # print("RES:", ret)
            # tasks.append(ret)

            # gc.collect()

        while True:
            for i, task in enumerate(tasks):
                if task.done():
                    del tasks[i]
            time.sleep(.2)
            
            if len(tasks) == 0:
                break

        print("LEN:", len(tasks))

        # for f in tasks:
        #     print("GETTING", f)
        #     print(f.result())

        # secede()
        client.gather(tasks)
        # rejoin()
        return self.node.len()

    def __repr__(self):
        return self.node.__class__.__name__ + str("_STREAM")


class Workflow:

    def __init__(self, name="", processes=None):
        super(Workflow, self).__init__()

        self._processes = []
        self._observers = set()
        self._process_map = {}
        self.name = name

        self._unmatched = []
        self._streams = []

        if processes:
            self._processes.extend(processes)
        self.staged = False

        self.lastresult = []
        self.disabled = False

        self.graph_exec = None

    def add(self, process: ProcessingPlugin, name: str = "", auto_connect_all: bool = False):
        """
        Adds a Process as a child.
        Parameters
        ----------
        process:    ProcessingPlugin
            Process to add
        name: str
            Optional name to give a process
        auto_connect_all: bool
            If True, connects Outputs of the previously added Process to the Inputs of process, matching names and types
        """
        self._processes.append(process)

        # if a name is given
        if name is not None or len(name) > 0:
            self._process_map[name] = process
            self.__dict__[name] = process

        process.attach(self.update)
        self.update()

        if auto_connect_all: self.autoConnectAll()

        return self

    def __getitem__(self, item):

        if item in self._process_map:
            return self._process_map[item]

        return None

    def connect(self, _out, _in):
        """
        Connect output to input
        :param _out:
        :param _in:
        :return:
        """
        if isinstance(_out, Var) and isinstance(_in, Var):
            _out.connect(_in)
        elif isinstance(_out, ProcessingPlugin) and isinstance(_in, ProcessingPlugin):
            inputprocess = _in
            outputprocess = _out

            # for each input of given process
            for input in inputprocess.inputs.values():
                for output in outputprocess.outputs.values():
                    # if matching name
                    if output.name == input.name and output.type == input.type:
                        print("CONNECTING:", outputprocess, output, inputprocess, input)
                        output.connect(input)
                        break

    def convert(self):
        """

        :return:

        """

        def find_unmatched_tasks():
            """
            loop over processes and find tasks that either don't have inputs
            or don't match with something in the same graph..
            """

            unmatched_tasks = set()

            for process in self._processes:
                for input in process.inputs.values():
                    for _, mapped_output in input.map_inputs:
                        if mapped_output.parent not in self._processes:
                            unmatched_tasks.add(mapped_output.parent)

            return unmatched_tasks

        unmatched_tasks = find_unmatched_tasks()

        for match in unmatched_tasks:
            self._processes.append(match)
            self._unmatched.append(unmatched_tasks)

        print(self._processes)
        graph = self.convertGraph()
        return graph[0], graph[1], self._unmatched

    def stream(self, process1, process2):
        """
        stream data from process1 to process2
        :param process1:
        :param process2:
        :return:
        """

        if isinstance(process2, Workflow):
            print("IsWorkflow")
            execute_graph, start_tasks, unmatched = process2.convert()
            print(execute_graph, start_tasks, unmatched)
            stream_graph = {}
            wsp = WorkflowStreamProcess(process1, None, execute_graph, start_tasks)
            stream_graph["0"] = (wsp, (None,))

            self.graph_exec = stream_graph, ["0"]

        elif isinstance(process2, ProcessingPlugin):
            print("IsPlugin")
        else:
            raise Exception("Neither Workflow nor plugin connected")

        return None

    def findEndTasks(self):
        """
        find tasks at the end of the graph and work up
        check inputs and remove dependency nodes, what is left is unique ones
        """

        dependent_tasks = set()

        for process in self.processes:
            for input in process.inputs.values():
                for _, mapped_output in input.map_inputs:
                    dependent_tasks.add(mapped_output.parent)

        end_tasks = set(self.processes) - dependent_tasks

        msg.logMessage('End tasks:', *[task.name for task in end_tasks], msg.DEBUG)
        return end_tasks

    def generateGraph(self, dsk, node, mapped_node):
        """
        Recursive function that adds
        :param dsk:
        :param q:
        :param node:
        :param mapped_node:
        :return:
        """
        if node in mapped_node:
            return

        mapped_node.append(node)

        args = OrderedDict()
        named_args = {}

        for inp in node.inputs.keys():
            for input_map in node.inputs[inp].map_inputs:
                self.generateGraph(dsk, input_map[1].parent, mapped_node)
                args[input_map[1].parent.id] = None
                # named_args.append({input_map[1].name: input_map[0]})  # TODO test to make sure output is in input
                named_args[input_map[1].name] = input_map[0]

        workflow = WorkflowProcess(node, named_args)
        dsk[node.id] = tuple([workflow, list(reversed(args.keys()))])

    def convertGraph(self):
        """
        process from end tasks and into all dependent ones
        """

        # if graph already generated then return
        if self.graph_exec is not None:
            return self.graph_exec

        for (i, node) in enumerate(self._processes):
            node.id = str(i)

        end_tasks = self.findEndTasks()

        graph = {}
        mapped_node = []

        for task in end_tasks:
            self.generateGraph(graph, task, mapped_node)

        self.graph_exec = graph, [i.id for i in end_tasks]

        return self.graph_exec

    def addProcess(self, process: ProcessingPlugin, autoconnectall: bool = False):
        """
        Adds a Process as a child.
        Parameters
        ----------
        process:    ProcessingPlugin
            Process to add
        autowireup: bool
            If True, connects Outputs of the previously added Process to the Inputs of process, matching names and types
        """
        self._processes.append(process)
        process.attach(self.update)
        self.update()
        if autoconnectall: self.autoConnectAll()
        self.update()

    def insertProcess(self, index: int, process: ProcessingPlugin, autoconnectall: bool = False):
        self._processes.insert(index, process)
        process.attach(self.update)
        self.update()
        if autoconnectall: self.autoConnectAll()

    def removeProcess(self, process: ProcessingPlugin = None, index=None, autoconnectall=False):
        if not process: process = self._processes[index]
        self._processes.remove(process)
        process.detach()
        process._workflow = None
        if autoconnectall: self.autoConnectAll()
        self.update()

    def autoConnectAll(self):
        self.clearConnections()

        # for each process
        for inputprocess in self.processes:

            # for each input of given process
            for input in inputprocess.inputs.values():
                bestmatch = None
                matchness = 0
                # Parse backwards from the given process, looking for matching outputs
                for outputprocess in reversed(self.processes[:self.processes.index(inputprocess)]):
                    # check each output
                    for output in outputprocess.outputs.values():
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
                        f'connected {bestmatch.parent.__class__.__name__}:{bestmatch.name} to {input.parent.__class__.__name__}:{input.name}',
                        level=msg.DEBUG)

    def clearConnections(self):
        for process in self.processes:
            process.clearConnections()

    def toggleDisableProcess(self, process, autoconnectall=False):
        process.disabled = not process.disabled
        process.clearConnections()
        if autoconnectall: self.autoConnectAll()
        self.update()

    @property
    def processes(self) -> List[ProcessingPlugin]:
        """
        res = []
        for process in self._processes:
            if isinstance(process, Workflow):
                res += process.processes
            elif not process.disabled:
                res.append(process)

        return res
        """
        return [process for process in self._processes if not process.disabled]

    @processes.setter
    def processes(self, processes):
        for process in self._processes:
            process._workflow = None

        self._processes = processes
        for process in processes:
            process._workflow = self
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
        # TODO: Processes invalidate parent workflow staged attribute if data resources are modified, but not parameters
        # TODO: check if data is accessible from compute resource; if not -> copy data to compute resource
        # TODO: use cam-link to mirror installation of plugin packages

    def execute(self, connection, callback_slot=None, finished_slot=None, except_slot=None, default_exhandle=True,
                lock=None, fill_kwargs=True, threadkey=None, **kwargs):
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

        future = QThreadFuture(execution.executor.execute, self,
                               callback_slot=callback_slot,
                               finished_slot=finished_slot,
                               default_exhandle=default_exhandle,
                               lock=lock,
                               threadkey=threadkey)
        future.result()
        return future

    def execute_all(self, connection, callback_slot=None, finished_slot=None, except_slot=None, default_exhandle=True,
                    lock=None, fill_kwargs=True, threadkey=None, **kwargs):
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

        def executeiterator(workflow):
            for kwargvalues in zip(*kwargs.values()):
                zipkwargs = dict(zip(kwargs.keys(), kwargvalues))
                if fill_kwargs:
                    self.fillKwargs(**zipkwargs)
                yield execution.executor.execute(workflow)

        future = QThreadFutureIterator(executeiterator, self,
                                       callback_slot=callback_slot,
                                       finished_slot=finished_slot,
                                       default_exhandle=default_exhandle,
                                       lock=lock,
                                       threadkey=threadkey)
        future.start()
        return future

    def fillKwargs(self, **kwargs):
        """
        Fills in all empty inputs with names matching keys in kwargs.
        """
        for process in self.processes:
            for key, input in process.inputs.items():
                if not input.map_inputs and key in kwargs:
                    input.value = kwargs[key]

    def validate(self):
        """
        Validate all of:
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

    def update(self, *args, **kwargs):
        for observer in self._observers:
            observer()
