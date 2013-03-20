#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import logging
import socket
import traceback
import unittest

import eventlet.debug
import eventlet.hubs.hub
import eventlet.semaphore
import junction
import junction.errors
from junction.core import backend


TIMEOUT = 0.015
STARTPORT = 9000

GTL = eventlet.semaphore.Semaphore(1)

def _free_port():
    port = STARTPORT
    while 1:
        try:
            socket.socket().bind(("127.0.0.1", port))
        except socket.error:
            port += 1
        else:
            return port

class EventletTestCase(unittest.TestCase):
    def setUp(self):
        GTL.acquire()
        junction.activate_eventlet()
        eventlet.hubs.hub.g_prevent_multiple_readers = False
        self._tbprint = traceback.print_exception

    def tearDown(self):
        GTL.release()


class JunctionTests(object):
    def create_hub(self, peers=None):
        peer = junction.Hub(("127.0.0.1", _free_port()), peers or [])
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
        del self.peer, self.sender

        junction.errors.HANDLED_ERROR_TYPES = self._handled_errors_copy

        super(JunctionTests, self).tearDown()

    def test_publish_success(self):
        results = []
        ev = backend.Event()

        @self.peer.accept_publish("service", 0, 0, "method")
        def handler(item):
            results.append(item)
            if len(results) == 4:
                ev.set()

        for i in xrange(4):
            backend.pause()

        self.sender.publish("service", 0, "method", (1,), {})
        self.sender.publish("service", 0, "method", (2,), {})
        self.sender.publish("service", 0, "method", (3,), {})
        self.sender.publish("service", 0, "method", (4,), {})

        ev.wait(TIMEOUT)

        self.assertEqual(results, [1, 2, 3, 4])

    def test_publish_ruled_out_by_service(self):
        results = []
        ev = backend.Event()

        @self.peer.accept_publish("service1", 0, 0, "method")
        def handler(item):
            results.append(item)
            ev.set()

        for i in xrange(4):
            backend.pause()

        try:
            self.sender.publish("service2", 0, "method", (1,), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        assert ev.wait(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_method(self):
        results = []
        ev = backend.Event()

        @self.peer.accept_publish("service", 0, 0, "method1")
        def handler(item):
            results.append(item)
            ev.set()

        for i in xrange(4):
            backend.pause()

        try:
            self.sender.publish("service", 0, "method2", (1,), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        assert ev.wait(TIMEOUT)

        self.assertEqual(results, [])

    def test_publish_ruled_out_by_routing_id(self):
        results = []
        ev = backend.Event()

        # only sign up for even routing ids
        @self.peer.accept_publish("service", 1, 0, "method")
        def handler(item):
            results.append(item)
            ev.set()

        for i in xrange(4):
            backend.pause()

        try:
            self.sender.publish("service", 1, "method", (1,), {})
        except junction.errors.Unroutable:
            # eat this as Clients don't get this raised, only Hubs
            pass

        assert ev.wait(TIMEOUT)

        self.assertEqual(results, [])

    def test_chunked_publish_success(self):
        results = []
        ev = backend.Event()

        @self.peer.accept_publish("service", 0, 0, "method")
        def handler(items):
            for item in items:
                results.append(item)
            ev.set()

        for i in xrange(4):
            backend.pause()

        self.sender.publish("service", 0, "method", ((x for x in xrange(5)),))

        assert not ev.wait(TIMEOUT)

        self.assertEqual(results, [0, 1, 2, 3, 4])

    def test_rpc_success(self):
        handler_results = []
        sender_results = []

        @self.peer.accept_rpc("service", 0, 0, "method")
        def handler(x):
            handler_results.append(x)
            return x ** 2

        for i in xrange(4):
            backend.pause()

        sender_results.append(self.sender.rpc("service", 0, "method", (1,), {},
            timeout=TIMEOUT))
        sender_results.append(self.sender.rpc("service", 0, "method", (2,), {},
            timeout=TIMEOUT))
        sender_results.append(self.sender.rpc("service", 0, "method", (3,), {},
            timeout=TIMEOUT))
        sender_results.append(self.sender.rpc("service", 0, "method", (4,), {},
            timeout=TIMEOUT))

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])

    def test_rpc_ruled_out_by_service(self):
        results = []
        ev = backend.Event()

        @self.peer.accept_rpc("service1", 0, 0, "method")
        def handler(item):
            results.append(item)
            ev.set()

        for i in xrange(4):
            backend.pause()

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service2", 0, "method", (1,), {}, TIMEOUT)

        assert ev.wait(TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_method(self):
        results = []

        self.peer.accept_rpc("service", 0, 0, "method1", results.append)

        for i in xrange(4):
            backend.pause()

        result = self.sender.rpc("service", 0, "method2", (1,), {}, TIMEOUT)
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], junction.errors.UnsupportedRemoteMethod)

        self.assertEqual(results, [])

    def test_rpc_ruled_out_by_routing_id(self):
        results = []

        self.peer.accept_rpc("service", 1, 0, "method", results.append)

        for i in xrange(4):
            backend.pause()

        self.assertRaises(junction.errors.Unroutable,
                self.sender.rpc, "service", 1, "method", (1,), {}, TIMEOUT)

        self.assertEqual(results, [])

    def test_rpc_handler_recognized_exception(self):
        class CustomError(junction.errors.HandledError):
            code = 3

        def handler():
            raise CustomError("gaah")

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        for i in xrange(4):
            backend.pause()

        result = self.sender.rpc("service", 0, "method", (), {}, TIMEOUT)

        self.assertEqual(len(result), 1)
        self.assert_(isinstance(result[0], CustomError), junction.errors.HANDLED_ERROR_TYPES)
        self.assertEqual(result[0].args[0], self.connection.addr)
        self.assertEqual(result[0].args[1], "gaah")

    def test_rpc_handler_unknown_exception(self):
        class CustomError(Exception):
            pass

        def handler():
            raise CustomError("WOOPS")

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        for i in xrange(4):
            backend.pause()

        result = self.sender.rpc("service", 0, "method", (), {}, TIMEOUT)

        self.assertEqual(len(result), 1)
        self.assert_(isinstance(result[0], junction.errors.RemoteException))
        self.assertEqual(result[0].args[0], self.connection.addr)
        self.assertEqual(result[0].args[1].splitlines()[-1], "CustomError: WOOPS")

    def test_async_rpc_success(self):
        handler_results = []
        sender_results = []

        def handler(x):
            handler_results.append(x)
            return x ** 2

        self.peer.accept_rpc("service", 0, 0, "method", handler)

        for i in xrange(4):
            backend.pause()

        rpcs = []

        rpcs.append(self.sender.send_rpc("service", 0, "method", (1,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (2,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (3,), {}))
        rpcs.append(self.sender.send_rpc("service", 0, "method", (4,), {}))

        while rpcs:
            rpc = self.sender.wait_any(rpcs, TIMEOUT)
            rpcs.remove(rpc)
            sender_results.append(rpc.value)

        self.assertEqual(handler_results, [1, 2, 3, 4])
        self.assertEqual(sender_results, [[1], [4], [9], [16]])

    def test_singular_rpc(self):
        handler_results = []
        sender_results = []

        @self.peer.accept_rpc("service", 0, 0, "method")
        def handler(x):
            handler_results.append(x)
            return x ** 2

        for i in xrange(4):
            backend.pause()

        sender_results.append(self.sender.rpc("service", 0, "method", (1,), {},
            timeout=TIMEOUT, singular=True))
        sender_results.append(self.sender.rpc("service", 0, "method", (2,), {},
            timeout=TIMEOUT, singular=True))
        sender_results.append(self.sender.rpc("service", 0, "method", (3,), {},
            timeout=TIMEOUT, singular=True))
        sender_results.append(self.sender.rpc("service", 0, "method", (4,), {},
            timeout=TIMEOUT, singular=True))

        self.assertEqual(handler_results, [1,2,3,4])
        self.assertEqual(sender_results, [1,4,9,16])

    def test_chunked_publish(self):
        results = []

        @self.peer.accept_rpc('service', 0, 0, 'method')
        def handler(chunks):
            for chunk in chunks:
                results.append(chunk)
            return 5

        for i in xrange(4):
            backend.pause()

        def gen():
            yield 1
            yield 2

        self.assertEqual(
                self.sender.rpc('service', 0, 'method', (gen(),),
                    timeout=TIMEOUT),
                [5])

        self.assertEqual(results, [1,2])


class HubTests(JunctionTests, EventletTestCase):
    def build_sender(self):
        self.sender = junction.Hub(("127.0.0.1", 8000), [self.peer.addr])
        self.sender.start()
        self.sender.wait_connected()

    def test_publish_unroutable(self):
        self.assertRaises(junction.errors.Unroutable,
                self.sender.publish, "service", "method", 0, (), {})

    def test_rpc_receiver_count_includes_self(self):
        @self.peer.accept_rpc('service', 0, 0, 'method')
        def handler():
            return 8

        @self.sender.accept_rpc('service', 0, 0, 'method')
        def handler():
            return 9

        backend.pause_for(TIMEOUT)

        self.assertEqual(2,
                self.sender.rpc_receiver_count('service', 0))

    def test_publish_receiver_count_includes_self(self):
        @self.peer.accept_publish('service', 0, 0, 'method')
        def handler():
            return 8

        @self.sender.accept_publish('service', 0, 0, 'method')
        def handler():
            return 9

        backend.pause_for(TIMEOUT)

        self.assertEqual(2,
                self.sender.publish_receiver_count('service', 0))


class ClientTests(JunctionTests, EventletTestCase):
    def build_sender(self):
        self.sender = junction.Client(self.peer.addr)
        self.sender.connect()
        self.sender.wait_connected()


class RelayedClientTests(JunctionTests, EventletTestCase):
    def build_sender(self):
        self.relayer = junction.Hub(
                ("127.0.0.1", self.peer.addr[1] + 1), [self.peer.addr])
        self.relayer.start()
        self.connection = self.relayer

        self.sender = junction.Client(self.relayer.addr)
        self.sender.connect()

        self.relayer.wait_connected()
        self.sender.wait_connected()

    def tearDown(self):
        self.relayer.shutdown()
        super(RelayedClientTests, self).tearDown()


class NetworklessDependentTests(EventletTestCase):
    def test_some_math(self):
        fut = junction.Future()
        fut.finish(4)
        dep = fut.after(
                lambda x: x * 3).after(
                lambda x: x - 7).after(
                lambda x: x ** 3).after(
                lambda x: x // 2)
        dep.wait(TIMEOUT)
        self.assertEqual(dep.value, 62)


class DownedConnectionTests(EventletTestCase):
    def kill_client(self, cli_list):
        cli = cli_list.pop()
        cli._peer.sock.close()

    def kill_hub(self, hub_list):
        hub = hub_list.pop()
        for peer in hub._dispatcher.peers.values():
            peer.sock.close()

    def test_unrelated_rpcs_are_unaffected(self):
        hub = junction.Hub(("127.0.0.1", _free_port()), [])

        @hub.accept_rpc('service', 0, 0, 'method')
        def handle():
            backend.pause_for(TIMEOUT)
            return 1

        hub.start()

        peer = junction.Hub(("127.0.0.1", _free_port()), [hub.addr])
        peer.start()
        peer.wait_connected()

        client = junction.Client(hub.addr)
        client.connect()
        client.wait_connected()
        client = [client]

        backend.schedule(self.kill_client, (client,))

        # hub does a self-rpc during which the client connection goes away
        result = peer.rpc('service', 0, 'method', singular=1)

        self.assertEqual(result, 1)

    def test_unrelated_self_rpcs_are_unaffected(self):
        hub = junction.Hub(("127.0.0.1", _free_port()), [])

        @hub.accept_rpc('service', 0, 0, 'method')
        def handle():
            backend.pause_for(TIMEOUT)
            return 1

        hub.start()

        client = junction.Client(hub.addr)
        client.connect()
        client.wait_connected()
        client = [client]

        @backend.schedule
        def kill_client():
            # so it'll get GC'd
            cli = client.pop()
            cli._peer.sock.close()

        # hub does a self-rpc during which the client connection goes away
        result = hub.rpc('service', 0, 'method', singular=1)

        self.assertEqual(result, 1)

    def test_unrelated_client_chunked_publishes_are_unrelated(self):
        port = _free_port()
        hub = junction.Hub(("127.0.0.1", port), [])

        d = {}

        @hub.accept_publish('service', 0, 0, 'method')
        def handle(x, source):
            for item in x:
                d.setdefault(source, 0)
                d[source] += 1

        hub.start()

        c1 = junction.Client(("127.0.0.1", port))
        c1.connect()
        c1.wait_connected()
        c2 = junction.Client(("127.0.0.1", port))
        c2.connect()
        c2.wait_connected()

        def gen():
            backend.pause_for(TIMEOUT)
            yield None
            backend.pause_for(TIMEOUT)
            yield None
            backend.pause_for(TIMEOUT)
            yield None

        backend.schedule(c1.publish, args=('service', 0, 'method'),
                kwargs={'args': (gen(),), 'kwargs': {'source': 'a'}})
        backend.schedule(c2.publish, args=('service', 0, 'method'),
                kwargs={'args': (gen(),), 'kwargs': {'source': 'b'}})

        backend.pause_for(TIMEOUT)

        c2 = [c2]
        self.kill_client(c2)

        backend.pause_for(TIMEOUT)
        backend.pause_for(TIMEOUT)
        backend.pause_for(TIMEOUT)

        self.assertEquals(d, {'a': 3, 'b': 1})

    def test_downed_hub_during_chunked_publish_terminates_correctly(self):
        port = _free_port()
        hub = junction.Hub(("127.0.0.1", port), [])
        l = []
        ev = backend.Event()

        @hub.accept_publish('service', 0, 0, 'method')
        def handle(x):
            for item in x:
                l.append(item)
            ev.set()

        hub.start()

        port2 = _free_port()
        hub2 = junction.Hub(("127.0.0.1", port2), [("127.0.0.1", port)])
        hub2.start()
        hub2.wait_connected()
        hub2 = [hub2]

        def gen():
            yield 1
            yield 2
            self.kill_hub(hub2)

        hub2[0].publish('service', 0, 'method', (gen(),))
        ev.wait(TIMEOUT)

        self.assertEqual(l[:2], [1,2])
        self.assertEqual(len(l), 3, l)
        self.assertIsInstance(l[-1], junction.errors.LostConnection)

    def test_downed_hub_during_chunk_pub_to_client_terminates_correctly(self):
        port = _free_port()
        hub = junction.Hub(("127.0.0.1", port), [])
        l = []
        ev = backend.Event()

        @hub.accept_publish('service', 0, 0, 'method')
        def handle(x):
            for item in x:
                l.append(item)
            ev.set()

        hub.start()

        client = junction.Client(("127.0.0.1", port))
        client.connect()
        client.wait_connected()
        client = [client]

        def gen():
            yield 1
            yield 2
            self.kill_client(client)

        client[0].publish('service', 0, 'method', (gen(),))
        ev.wait(TIMEOUT)

        self.assertEqual(l[:2], [1,2])
        self.assertEqual(len(l), 3)
        self.assertIsInstance(l[-1], junction.errors.LostConnection)

    def test_downed_recipient_cancels_the_hub_sender_during_chunked_publish(self):
        port = _free_port()
        hub = junction.Hub(("127.0.0.1", port), [])
        triggered = [False]

        @hub.accept_publish('service', 0, 0, 'method')
        def handle(chunks):
            for item in chunks:
                pass

        hub.start()

        port2 = _free_port()
        hub2 = junction.Hub(("127.0.0.1", port2), [("127.0.0.1", port)])
        hub2.start()
        hub2.wait_connected()

        def gen():
            try:
                while 1:
                    yield None
                    backend.pause_for(TIMEOUT)
            finally:
                triggered[0] = True

        hub2.publish('service', 0, 'method', (gen(),))

        hub = [hub]
        backend.schedule_in(TIMEOUT * 4, self.kill_hub, args=(hub,))

        backend.pause_for(TIMEOUT * 5)

        assert triggered[0]

    def test_downed_recipient_cancels_the_hub_sender_during_chunked_request(self):
        port = _free_port()
        hub = junction.Hub(("127.0.0.1", port), [])
        triggered = [False]

        @hub.accept_rpc('service', 0, 0, 'method')
        def handle(chunks):
            for item in chunks:
                pass
            return "all done"

        hub.start()

        port2 = _free_port()
        hub2 = junction.Hub(("127.0.0.1", port2), [("127.0.0.1", port)])
        hub2.start()
        hub2.wait_connected()

        def gen():
            try:
                while 1:
                    yield None
                    backend.pause_for(TIMEOUT)
            finally:
                triggered[0] = True

        rpc = hub2.send_rpc('service', 0, 'method', (gen(),))

        hub = [hub]
        backend.schedule_in(TIMEOUT * 4, self.kill_hub, args=(hub,))

        backend.pause_for(TIMEOUT * 5)

        assert triggered[0]


if __name__ == '__main__':
    unittest.main()
