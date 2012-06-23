import logging
import random
import sys
import time
import traceback

import greenhouse
import junction
import mummy


'''
Todo
----

- mapper function should be called once for a given piece of
  input and yield (key, val) pairs
- job config should specify a base case for reduction
- the mapper node should pick the reducer to send a yielded key/val
  pair to with `reducers[hash(key) % num_reducers]`, where reducers
  is *the same* list that was sent to all mappers by the coordinator.
- no failure case should cause the client to indefinitely hang

Done
----

- mapper's data source is provided at job-start time
- data source takes a 'mapper id' which the mapper was instantiated with
'''

def map_reduce(hub, service, source, mapper, reducer):
    return hub.rpc(service, 0, "map_reduce",
            (dump_func(source), dump_func(mapper), dump_func(reducer)), {})


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
            "%s-result" % (service,),
            mask,
            value,
            "result",
            self.handle_result,
            schedule=False)

    def handle_map_reduce(self, dumped_source, dumped_mapper, dumped_reducer):
        reducer = random.randrange(self._num_reducers)

        mappers = self._hub.publish_receiver_count(
                "%s-mapper" % (self._service,), 0)

        job_id = self._hub.rpc(
            "%s-reducer" % (self._service,),
            reducer,
            "setup",
            (dumped_reducer, mappers, self._value),
            {})[0]

        self._hub.publish(
            "%s-mapper" % (self._service,),
            0,
            "map",
            (job_id, dumped_source, dumped_mapper, reducer),
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
                "%s-reducer" % (service,),
                mask,
                value,
                "setup",
                self.handle_setup,
                schedule=False)

        hub.accept_publish(
                "%s-reducer" % (service,),
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
                        "%s-result" % (self._service,),
                        coordinator,
                        "result",
                        (self._value, job_id, self._results.pop(job_id),),
                        {})


class Mapper(object):
    page_size = 64

    def __init__(self, hub, service, mapper_id):
        self._hub = hub
        self._service = service
        self._mapper_id = mapper_id

        hub.accept_publish("%s-mapper" % (service,),
                0, 0, "map", self.handle_map, schedule=True)

    def handle_map(self, job_id, dumped_datasource, dumped_mapper, reducer_id):
        ds_func = load_func(dumped_datasource)
        items = ds_func(self._mapper_id)
        map_func = load_func(dumped_mapper)
        results = []

        for i, item in enumerate(items):
            results.append(map_func(item))

            if i and not i % self.page_size:
                self._hub.publish(
                    "%s-reducer" % (self._service,),
                    reducer_id,
                    "reduce",
                    (job_id, results, False),
                    {})
                results = []
                greenhouse.pause()

        self._hub.publish(
            "%s-reducer" % (self._service,),
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


if __name__ == '__main__':
    greenhouse.global_exception_handler(traceback.print_exception)
    junction.configure_logging(level=1)

    if sys.argv[1] == "mapper":
        hub = junction.Hub(("", 9090), [("", 9091), ("", 9092)])
        hub.start()
        mapper = Mapper(hub, 'map-reduce', 1)
        greenhouse.run_backdoor(("", 8090), locals())
    elif sys.argv[1] == "reducer":
        hub = junction.Hub(("", 9091), [("", 9090), ("", 9092)])
        hub.start()
        reducer = Reducer(hub, 'map-reduce', 0, 0)
        greenhouse.run_backdoor(("", 8091), locals())
    elif sys.argv[1] == "coordinator":
        hub = junction.Hub(("", 9092), [("", 9090), ("", 9091)])
        hub.start()
        coord = Coordinator(hub, 'map-reduce', 0, 0, 1)
        greenhouse.run_backdoor(("", 8092), locals())
    elif sys.argv[1] == "client":
        cli = junction.Client(("", 9092)) # only need the coordinator
        cli.connect()
        cli.wait_on_connections()
        result = map_reduce(cli, 'map-reduce',
                lambda i: range(1,8),
                lambda x: x ** 2,
                lambda a, b: a + b)
        print repr(result)
    else:
        print >> sys.stderr, \
                "argument must be 'mapper', 'reducer', or 'coordinator'"
        sys.exit(1)
