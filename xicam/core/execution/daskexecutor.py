from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
from dask.diagnostics import visualize
from xicam.core import msg
from appdirs import user_config_dir
import distributed
from distributed import Queue
from .workflow import WorkflowStreamProcess


class DaskExecutor(object):
    def __init__(self):
        super(DaskExecutor, self).__init__()
        self.client = None
        self.parameters = {}

    def update_parameters(self, task, name, value):
        if self.client is None:
            return

        parameters = {}
        #parameters[task.name]

        try:
            print("ASD", task, name, value)
            parameters[task.name] = (name, value)
            self.parameters.update(parameters)

            self.client.unpublish_dataset("parameters")
            self.client.publish_dataset(parameters=self.parameters)
        except:
            pass
        pass

    def execute(self, wf, callback, client=None):
        if not wf.processes:
            return {}

        if client is None:
            if self.client is None:
                self.client = distributed.Client()
            client = self.client
        else:
            self.client = client

        dsk = wf.convertGraph()
        print("GRAPH_EXEC", dsk[0], dsk[1])

        try:
            client.publish_dataset(parameters=self.parameters)
        except:
            pass

        my_queue_list = []
        for key in dsk[0]:
            if isinstance(dsk[0][key][0], WorkflowStreamProcess):
                my_queue = Queue("MYQ")
                my_queue_list.append(my_queue)
                dsk[0][key][0].queue = my_queue
                print("Auto assigning:", my_queue)

        tasks = wf.getTasks()

        print(tasks)

        queue_map = []

        queue_map.append(my_queue)

        #for task in tasks:
        #    outputs = task.outputs
        #    for output_key in outputs:
        #        output = outputs[output_key]
        #        print(task, output.name, output.visualize)

        #        if output.visualize is True:
        #            output.queue = Queue()
        #            queue_map.append(output)

        # find all tasks with outputs that have callbacks

        #print(dsk[0]["0"][0])
        #dsk[0]["0"][0].queue = my_queue

        #def emit(**args):
        #    print("emitting")

        #dsk[0]["0"][0].emit = emit

        result = client.get(dsk[0], dsk[1], sync=False)

        #len = my_queue.get()

        my_queues = []

        print("RESULT!!", result)

        import cloudpickle

        while not result[0].done():
            for queue_var in queue_map:
                #queue = queue_var.queue
                queue = queue_var
                while queue.qsize() > 0:
                    res = queue.get()
                    name = res[0]

                    print("GOT:", name)
                    if callback is not None:
                        value = res[1]
                        data = cloudpickle.loads(value)
                        callback((name, data))


        """
        for res in range(len):
            x = my_queue.get()
            yres = my_queue.get()
            print(":::", x, yres, len, res)

        print("RESULT", result, my_queues)
        for f in my_queues:
            print(f.result())

        """

        # import cloudpickle

        """
        output_list = []
        end_loop = False
        
        while (not end_loop) or (len(output_list) > 0):
            while my_queue.qsize() > 0:
                res = my_queue.get()

                # not a Future, must be exit...
                if isinstance(res, str):
                    print("ENDING LOOP")
                    end_loop = True
                    break
                else:
                    #data = cloudpickle.loads(res)
                    output_list.append(res)

            for j, f in enumerate(output_list):
                print("Checking:", f.done())
                if f.done() and callback is not None:
                    print("in here", f.result())
                    data = f.result()["recon"].value
                    callback(data)
                    del output_list[j]
            #print("CLIENT", data)
        """

        print("Finish", result)
        # print("HERE", result[0].result())

        wf.lastresult = result

        return result

    def execute_backup(self, wf, client=None):
        if not wf.processes:
            return {}

        if client is None:
            if self.client is None: self.client = distributed.Client()
            client = self.client

        dsk = wf.convertGraph()
        #result = client.get(dsk[0], dsk[1])

        # with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
        result = client.get(dsk[0], dsk[1])

        msg.logMessage('result:', result, level=msg.DEBUG)
        # path = user_config_dir('xicam/profile.html')
        # visualize([prof, rprof, cprof], show=False, file_path=path)
        # msg.logMessage(f'Profile saved: {path}')

        wf.lastresult = result

        return result
