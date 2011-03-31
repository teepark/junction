import random
try:
    import cPickle as pickle
except ImportError:
    import pickle

import greenhouse
import junction


class Coordinator(object):
    def __init__(self, node, service, mask, value, num_reducers):
        self._node = node
        self._service = service
        self._value = value
        self._num_reducers = num_reducers
        self._active = {}

        assert node.accept_rpc(
            service,
            "map_reduce",
            mask,
            value,
            self.handle_map_reduce,
            schedule=True), "illegal subscription (%r, %d, %d)" % (
                    service, mask, value)

        assert node.accept_publish(
            service,
            "result",
            mask,
            value,
            self.handle_result,
            schedule=False)

    def handle_map_reduce(self, pickled_mapper, pickled_reducer):
        reducer = random.randrange(self._num_reducers)

        # just get the count of mappers available
        mappers = len(self._node.rpc(
            self._service,
            "mapper_ping",
            0,
            (),
            {}))

        job_id = self._node.rpc(
            self._service,
            "setup",
            reducer,
            (pickled_reducer, mappers, self._value),
            {})[0]

        self._node.publish(
            self._service,
            "map",
            0,
            (job_id, pickled_mapper, reducer),
            {})

        self._active[(reducer, job_id)] = waiter = greenhouse.Event()
        waiter.wait()

        return self._active.pop((reducer, job_id))

    def handle_result(self, reducer, job_id, results):
        ev = self._active[(reducer, job_id)]
        self._active[(reducer, job_id)] = results
        ev.set()


class Reducer(object):
    def __init__(self, node, service, mask, value):
        self._node = node
        self._service = service
        self._value = value
        self._active = {}
        self._results = {}
        self._setup_data = {}
        self._counter = 1

        node.accept_rpc(
                service,
                "setup",
                mask,
                value,
                self.handle_setup,
                schedule=False)

        node.accept_publish(
                service,
                "reduce",
                mask,
                value,
                self.handle_reduce,
                schedule=True)

    def handle_setup(self, pickled_reducer, mapper_count, coordinator_id):
        counter = self._counter
        self._counter += 1

        self._setup_data[counter] = (coordinator_id, pickled_reducer)
        self._active[counter] = mapper_count

        return counter

    def handle_reduce(self, job_id, results, final):
        coordinator, pickled_reducer = self._setup_data[job_id]

        args = [pickle.loads(pickled_reducer), results]
        if job_id in self._results:
            args.append(self._results[job_id])

        self._results[job_id] = reduce(*args)

        if final:
            self._active[job_id] -= 1
            if not self._active[job_id]:
                del self._active[job_id]
                del self._setup_data[job_id]

                self._node.publish(
                        self._service,
                        "result",
                        coordinator,
                        (self._value, job_id, self._results.pop(job_id),),
                        {})


class Mapper(object):
    page_size = 64

    def __init__(self, node, service, data_source, reducer_count):
        self._node = node
        self._service = service
        self._datasource = data_source
        self._num_reducers = reducer_count

        node.accept_rpc(
                service, "mapper_ping", 0, 0, self.handle_ping, schedule=False)

        node.accept_publish(
                service, "map", 0, 0, self.handle_map, schedule=True)

    def handle_ping(self):
        return None

    def handle_map(self, job_id, pickled_mapper, reducer_id):
        items = self._datasource()
        map_func = pickle.loads(pickled_mapper)
        results = []

        for i, item in enumerate(items):
            results.append(map_func(item))

            if i and not i % self.page_size:
                self._node.publish(
                    self._service,
                    "reduce",
                    reducer_id,
                    (job_id, results, False),
                    {})
                results = []
                greenhouse.pause()

        self._node.publish(
            self._service,
            "reduce",
            reducer_id,
            (job_id, results, True),
            {})
