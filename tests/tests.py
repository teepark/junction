#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback
import unittest

import greenhouse
import junction
import junction.errors

#greenhouse.add_exception_handler(traceback.print_exception)


TIMEOUT = 0.002
PORT = 5000

GTL = greenhouse.Lock()

# base class stolen from the greenhouse test suite
class StateClearingTestCase(unittest.TestCase):
    def setUp(self):
        GTL.acquire()

        state = greenhouse.scheduler.state
        state.awoken_from_events.clear()
        state.timed_paused[:] = []
        state.paused[:] = []
        state.descriptormap.clear()
        state.to_run.clear()

        greenhouse.poller.set()

    def tearDown(self):
        GTL.release()


class JunctionTests(object):
    def create_node(self, peers=None):
        global PORT
        peer = junction.Node(("127.0.0.1", PORT), peers or [])
        PORT += 2
        peer.start()
        return peer

    def setUp(self):
        super(JunctionTests, self).setUp()

        self.peer = self.create_node()
        self.connection = self.peer

        self.build_sender()

        self._handled_errors_copy = junction.errors.HANDLED_ERROR_TYPES.copy()

    def tearDown(self):
        self.peer.shutdown()
        self.sender.shutdown()
        greenhouse.pause_for(TIMEOUT)
        del self.peer, self.sender

        junction.errors.HANDLED_ERROR_TYPES = self._handled_errors_copy

        super(JunctionTests, self).tearDown()

    def test_publish_success(self):
        results = []

        self.peer.accept_publish("service", "method", 0, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        self.sender.publish("service", "method", 0, (1,), {})
        self.sender.publish("service", "method", 0, (2,), {})
        self.sender.publish("service", "method", 0, (3,), {})
        self.sender.publish("service", "method", 0, (4,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [1, 2, 3, 4])

    def test_publish_ruled_out_by_service(self):
        results = []

        self.peer.accept_publish("service1", "method", 0, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service2", "method", 0, (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Nodes
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_method(self):
        results = []

        self.peer.accept_publish("service", "method1", 0, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service", "method2", 0, (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Nodes
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_routing_id(self):
        results = []

        # only sign up for even routing ids
        self.peer.accept_publish("service", "method", 1, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service", "method", 1, (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Nodes
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_success(self):
        handler_results = []
        sender_results = []

        def handler(x):
            handler_results.append(x)
            return x ** 2

        self.peer.accept_rpc("service", "method", 0, 0, handler)

        greenhouse.pause_for(TIMEOUT)

        sender_results.append(self.sender.rpc("service", "method", 0, (1,), {}))
        sender_results.append(self.sender.rpc("service", "method", 0, (2,), {}))
        sender_results.append(self.sender.rpc("service", "method", 0, (3,), {}))
        sender_results.append(self.sender.rpc("service", "method", 0, (4,), {}))

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])

    def test_rpc_ruled_out_by_service(self):
        results = []

        self.peer.accept_rpc("service1", "method", 0, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service2", "method", 0, (1,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_method(self):
        results = []

        self.peer.accept_rpc("service", "method1", 0, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service", "method2", 0, (1,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_routing_id(self):
        results = []

        self.peer.accept_rpc("service", "method", 1, 0, results.append)

        greenhouse.pause_for(TIMEOUT)

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service", "method", 1, (1,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_handler_recognized_exception(self):
        class CustomError(junction.errors.HandledError):
            code = 3

        def handler():
            raise CustomError("gaah")

        self.peer.accept_rpc("service", "method", 0, 0, handler)

        greenhouse.pause_for(TIMEOUT)

        result = self.sender.rpc("service", "method", 0, (), {})

        self.assertEqual(len(result), 1)
        self.assert_(isinstance(result[0], CustomError), junction.errors.HANDLED_ERROR_TYPES)
        self.assertEqual(result[0].args[0], self.connection.addr)
        self.assertEqual(result[0].args[1], "gaah")

    def test_rpc_handler_unknown_exception(self):
        class CustomError(Exception):
            pass

        def handler():
            raise CustomError("DAMMIT")

        self.peer.accept_rpc("service", "method", 0, 0, handler)

        greenhouse.pause_for(TIMEOUT)

        result = self.sender.rpc("service", "method", 0, (), {})

        self.assertEqual(len(result), 1)
        self.assert_(isinstance(result[0], junction.errors.RemoteException))
        self.assertEqual(result[0].args[0], self.connection.addr)
        self.assertEqual(result[0].args[1][-1], "CustomError: DAMMIT\n")

    def test_async_rpc_success(self):
        handler_results = []
        sender_results = []

        def handler(x):
            handler_results.append(x)
            return x ** 2

        self.peer.accept_rpc("service", "method", 0, 0, handler)

        greenhouse.pause_for(TIMEOUT)

        rpcs = []

        rpcs.append(self.sender.send_rpc("service", "method", 0, (1,), {}))
        rpcs.append(self.sender.send_rpc("service", "method", 0, (2,), {}))
        rpcs.append(self.sender.send_rpc("service", "method", 0, (3,), {}))
        rpcs.append(self.sender.send_rpc("service", "method", 0, (4,), {}))

        while rpcs:
            rpc = self.sender.wait_any_rpc(rpcs)
            rpcs.remove(rpc)
            sender_results.append(rpc.results)

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])


class NodeTests(JunctionTests, StateClearingTestCase):
    def build_sender(self):
        self.sender = junction.Node(("127.0.0.1", 8000), [self.peer.addr])
        self.sender.start()
        self.sender.wait_on_connections()

    def test_publish_unroutable(self):
        self.assertRaises(junction.errors.Unroutable,
                self.sender.publish, "service", "method", 0, (), {})


class ClientTests(JunctionTests, StateClearingTestCase):
    def build_sender(self):
        self.sender = junction.Client(self.peer.addr)
        self.sender.connect()
        self.sender.wait_on_connections()


class RelayedClientTests(JunctionTests, StateClearingTestCase):
    def build_sender(self):
        self.relayer = junction.Node(
                ("127.0.0.1", self.peer.addr[1] + 1), [self.peer.addr])
        self.relayer.start()
        self.connection = self.relayer

        self.sender = junction.Client(self.relayer.addr)
        self.sender.connect()
        self.sender.wait_on_connections()

    def tearDown(self):
        self.relayer.shutdown()
        super(RelayedClientTests, self).tearDown()


if __name__ == '__main__':
    unittest.main()
