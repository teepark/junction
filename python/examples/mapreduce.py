import random

import greenhouse
import mummy


def map_reduce(hub, service, mapper, reducer):
    return hub.rpc(service, 0, "map_reduce",
            (dump_func(mapper), dump_func(reducer)), {})[0]


class Coordinator(object):
    def __init__(self, hub, service, mask, value, num_reducers):
        self._hub = hub
        self._service = service
        self._value = value
        self._num_reducers = num_reducers
        self._active = {}

        hub.accept_rpc(
            service,
            mask,
            value,
            "map_reduce",
            self.handle_map_reduce,
            schedule=True)

        hub.accept_publish(
            service,
            mask,
            value,
            "result",
            self.handle_result,
            schedule=False)

    def handle_map_reduce(self, dumped_mapper, dumped_reducer):
        reducer = random.randrange(self._num_reducers)

        mappers = self._hub.publish_receiver_count(self._service, "map", 0)

        job_id = self._hub.rpc(
            self._service,
            reducer,
            "setup",
            (dumped_reducer, mappers, self._value),
            {})[0]

        self._hub.publish(
            self._service,
            0,
            "map",
            (job_id, dumped_mapper, reducer),
            {})

        self._active[(reducer, job_id)] = waiter = greenhouse.Event()
        waiter.wait()

        return self._active.pop((reducer, job_id))

    def handle_result(self, reducer, job_id, results):
        ev = self._active[(reducer, job_id)]
        self._active[(reducer, job_id)] = results
        ev.set()


class Reducer(object):
    def __init__(self, hub, service, mask, value):
        self._hub = hub
        self._service = service
        self._value = value
        self._active = {}
        self._results = {}
        self._setup_data = {}
        self._counter = 1

        hub.accept_rpc(
                service,
                mask,
                value,
                "setup",
                self.handle_setup,
                schedule=False)

        hub.accept_publish(
                service,
                mask,
                value,
                "reduce",
                self.handle_reduce,
                schedule=True)

    def handle_setup(self, dumped_reducer, mapper_count, coordinator_id):
        counter = self._counter
        self._counter += 1

        self._setup_data[counter] = (coordinator_id, dumped_reducer)
        self._active[counter] = mapper_count

        return counter

    def handle_reduce(self, job_id, results, final):
        coordinator, dumped_reducer = self._setup_data[job_id]

        args = [load_func(dumped_reducer), results]
        if job_id in self._results:
            args.append(self._results[job_id])

        self._results[job_id] = reduce(*args)

        if final:
            self._active[job_id] -= 1
            if not self._active[job_id]:
                del self._active[job_id]
                del self._setup_data[job_id]

                self._hub.publish(
                        self._service,
                        coordinator,
                        "result",
                        (self._value, job_id, self._results.pop(job_id),),
                        {})


class Mapper(object):
    page_size = 64

    def __init__(self, hub, service, data_source):
        self._hub = hub
        self._service = service
        self._datasource = data_source

        hub.accept_publish(
                service, 0, 0, "map", self.handle_map, schedule=True)

    def handle_map(self, job_id, dumped_mapper, reducer_id):
        items = self._datasource()
        map_func = load_func(dumped_mapper)
        results = []

        for i, item in enumerate(items):
            results.append(map_func(item))

            if i and not i % self.page_size:
                self._hub.publish(
                    self._service,
                    reducer_id,
                    "reduce",
                    (job_id, results, False),
                    {})
                results = []
                greenhouse.pause()

        self._hub.publish(
            self._service,
            reducer_id,
            "reduce",
            (job_id, results, True),
            {})


def dump_func(f):
    code = f.func_code
    return mummy.dumps((
            code.co_argcount,
            code.co_nlocals,
            code.co_stacksize,
            code.co_flags,
            code.co_code,
            code.co_consts,
            code.co_names,
            code.co_varnames,
            code.co_filename,
            code.co_name,
            code.co_firstlineno,
            code.co_lnotab,
            code.co_freevars,
            code.co_cellvars))

def load_func(s):
    def func():
        pass
    code = type(load_func.func_code)(*mummy.loads(s))
    func.func_code = code
    return func
