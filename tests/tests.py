#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback
import unittest

import greenhouse
import junction
import junction.errors


TIMEOUT = 0.002
PORT = 5000

GTL = greenhouse.Lock()

# base class stolen from the greenhouse test suite
class StateClearingTestCase(unittest.TestCase):
    def setUp(self):
        GTL.acquire()

        state = greenhouse.scheduler.state
        state.awoken_from_events.clear()
        state.timed_paused.clear()
        state.paused[:] = []
        state.descriptormap.clear()
        state.to_run.clear()

        greenhouse.reset_poller()

    def tearDown(self):
        GTL.release()


class JunctionTests(object):
    def create_hub(self, peers=None):
        global PORT
        peer = junction.Hub(("127.0.0.1", PORT), peers or [])
        PORT += 2
        peer.start()
        return peer

    def setUp(self):
        super(JunctionTests, self).setUp()

        self.peer = self.create_hub()
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

        self.peer.accept_publish("service", 0, 0, "method", results.append)

        greenhouse.pause_for(TIMEOUT)

        self.sender.publish("service", 0, "method", (1,), {})
        self.sender.publish("service", 0, "method", (2,), {})
        self.sender.publish("service", 0, "method", (3,), {})
        self.sender.publish("service", 0, "method", (4,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [1, 2, 3, 4])

    def test_publish_ruled_out_by_service(self):
        results = []

        self.peer.accept_publish("service1", 0, 0, "method", results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service2", 0, "method", (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_method(self):
        results = []

        self.peer.accept_publish("service", 0, 0, "method1", results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service", 0, "method2", (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_routing_id(self):
        results = []

        # only sign up for even routing ids
        self.peer.accept_publish("service", 1, 0, "method", results.append)

        greenhouse.pause_for(TIMEOUT)

        try:
            self.sender.publish("service", 1, "method", (), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_success(self):
        handler_results = []
        sender_results = []

        def handler(x):
            handler_results.append(x)
            return x ** 2

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        greenhouse.pause_for(TIMEOUT)

        sender_results.append(self.sender.rpc("service", 0, "method", (1,), {}))
        sender_results.append(self.sender.rpc("service", 0, "method", (2,), {}))
        sender_results.append(self.sender.rpc("service", 0, "method", (3,), {}))
        sender_results.append(self.sender.rpc("service", 0, "method", (4,), {}))

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])

    def test_rpc_ruled_out_by_service(self):
        results = []

        self.peer.accept_rpc("service1", 0, 0, "method", results.append)

        greenhouse.pause_for(TIMEOUT)

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service2", 0, "method", (1,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_method(self):
        results = []

        self.peer.accept_rpc("service", 0, 0, "method1", results.append)

        greenhouse.pause_for(TIMEOUT)

        result = self.sender.rpc("service", 0, "method2", (1,), {})
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], junction.errors.UnsupportedRemoteMethod)

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_routing_id(self):
        results = []

        self.peer.accept_rpc("service", 1, 0, "method", results.append)

        greenhouse.pause_for(TIMEOUT)

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service", 1, "method", (1,), {})

        greenhouse.pause_for(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_handler_recognized_exception(self):
        class CustomError(junction.errors.HandledError):
            code = 3

        def handler():
            raise CustomError("gaah")

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        greenhouse.pause_for(TIMEOUT)

        result = self.sender.rpc("service", 0, "method", (), {})

        self.assertEqual(len(result), 1)
        self.assert_(isinstance(result[0], CustomError), junction.errors.HANDLED_ERROR_TYPES)
        self.assertEqual(result[0].args[0], self.connection.addr)
        self.assertEqual(result[0].args[1], "gaah")

    def test_rpc_handler_unknown_exception(self):
        class CustomError(Exception):
            pass

        def handler():
            raise CustomError("DAMMIT")

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        greenhouse.pause_for(TIMEOUT)

        result = self.sender.rpc("service", 0, "method", (), {})

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

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        greenhouse.pause_for(TIMEOUT)

        rpcs = []

        rpcs.append(self.sender.send_rpc("service", 0, "method", (1,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (2,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (3,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (4,), {}))

        while rpcs:
            rpc = self.sender.wait_any(rpcs)
            rpcs.remove(rpc)
            sender_results.append(rpc.results)

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])


class HubTests(JunctionTests, StateClearingTestCase):
    def build_sender(self):
        self.sender = junction.Hub(("127.0.0.1", 8000), [self.peer.addr])
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
        self.relayer = junction.Hub(
                ("127.0.0.1", self.peer.addr[1] + 1), [self.peer.addr])
        self.relayer.start()
        self.connection = self.relayer

        self.sender = junction.Client(self.relayer.addr)
        self.sender.connect()
        self.sender.wait_on_connections()

    def tearDown(self):
        self.relayer.shutdown()
        super(RelayedClientTests, self).tearDown()


class NetworklessDependentTests(StateClearingTestCase):
    def test_some_math(self):
        client = junction.Client(())
        dep = client.dependency_root(
                lambda: 4).after(
                lambda x: x * 3).after(
                lambda x: x - 7).after(
                lambda x: x ** 3).after(
                lambda x: x // 2)
        self.assertEqual(dep.wait(), 62)


if __name__ == '__main__':
    unittest.main()
