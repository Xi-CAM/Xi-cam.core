from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
from dask.diagnostics import visualize
from xicam.core import msg
from appdirs import user_config_dir
import distributed
from distributed import Queue


class DaskExecutor(object):
    def __init__(self):
        super(DaskExecutor, self).__init__()
        self.client = None

    def execute(self, wf, client=None):
        if not wf.processes:
            return {}

        #dsk = dsk.convertGraphX()

        #scheduler = distributed.LocalCluster(processes=False)
        #client = distributed.Client(scheduler)

        dsk = wf.convertGraph()
        print("GRAPH_EXEC", dsk[0], dsk[1])

        my_queue = Queue()

        print(dsk[0]["0"][0])
        dsk[0]["0"][0].queue = my_queue

        def emit(**args):
            print("emitting")

        dsk[0]["0"][0].emit = emit

        result = client.get(dsk[0], dsk[1], sync=False)

        #len = my_queue.get()

        my_queues = []

        """
        for res in range(len):
            x = my_queue.get()
            yres = my_queue.get()
            print(":::", x, yres, len, res)

        print("RESULT", result, my_queues)
        for f in my_queues:
            print(f.result())

        """

        import cloudpickle

        while True:
            while my_queue.qsize() > 0:
                res = my_queue.get()
                data = cloudpickle.loads(res)

                print("CLIENT", data)

        print("HERE", result)
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
