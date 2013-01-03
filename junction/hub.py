from __future__ import absolute_import

import logging
import random
import socket
import time

from . import errors, futures
from .core import backend, connection, const, dispatch, rpc


log = logging.getLogger("junction.hub")


class Hub(object):
    'A hub in the server graph'
    def __init__(self, addr, peer_addrs, hostname=None, hooks=None):
        self.addr = addr
        self._ident = (hostname or addr[0], addr[1])
        self._peers = peer_addrs
        self._started_peers = {}
        self._closing = False
        self._listener_coro = None

        self._rpc_client = rpc.RPCClient()
        self._dispatcher = dispatch.Dispatcher(self._rpc_client, self, hooks)

    def wait_connected(self, conns=None, timeout=None):
        '''Wait for connections to be made and their handshakes to finish

        :param conns:
            a single or list of (host, port) tuples with the connections that
            must be finished before the method will return. defaults to all the
            peers the :class:`Hub` was instantiated with.
        :param timeout:
            maximum time to wait in seconds. with None, there is no timeout.
        :type timeout: float or None

        :returns:
            ``True`` if all connections were made, ``False`` one or more
            failed.
        '''
        if timeout:
            deadline = time.time() + timeout
        conns = conns or self._started_peers.keys()
        if not hasattr(conns, "__iter__"):
            conns = [conns]

        for peer_addr in conns:
            remaining = max(0, deadline - time.time()) if timeout else None
            if not self._started_peers[peer_addr].wait_connected(remaining):
                if timeout:
                    log.warn("connect wait timed out after %.2f seconds" % timeout)
                return False
        return True

    def shutdown(self):
        'Close all peer connections and stop listening for new ones'
        log.info("shutting down")

        for peer in self._dispatcher.peers.values():
            peer.go_down(reconnect=False)

        if self._listener_coro:
            backend.schedule_exception(
                    errors._BailOutOfListener(), self._listener_coro)

    def accept_publish(
            self, service, mask, value, method, handler=None, schedule=False):
        '''Set a handler for incoming publish messages

        :param service: the incoming message must have this service
        :type service: anything hash-able
        :param mask:
            value to be bitwise-and'ed against the incoming id, the result of
            which must mask the 'value' param
        :type mask: int
        :param value:
            the result of `routing_id & mask` must match this in order to
            trigger the handler
        :type value: int
        :param method: the method name
        :type method: string
        :param handler:
            the function that will be called on incoming matching messages
        :type handler: callable
        :param schedule:
            whether to schedule a separate greenlet running ``handler`` for
            each matching message. default ``False``.
        :type schedule: bool

        :raises:
            - :class:`ImpossibleSubscription
              <junction.errors.ImpossibleSubscription>` if there is no routing
              ID which could possibly match the mask/value pair
            - :class:`OverlappingSubscription
              <junction.errors.OverlappingSubscription>` if a prior publish
              registration that overlaps with this one (there is a
              service/method/routing id that would match *both* this *and* a
              previously-made registration).
        '''
        # support @hub.accept_publish(serv, mask, val, meth) decorator usage
        if handler is None:
            return lambda h: self.accept_publish(
                    service, mask, value, method, h, schedule)

        log.info("accepting publishes%s %r" % (
                " scheduled" if schedule else "",
                (service, (mask, value), method),))

        self._dispatcher.add_local_subscription(const.MSG_TYPE_PUBLISH,
                service, mask, value, method, handler, schedule)

        return handler

    def unsubscribe_publish(self, service, mask, value):
        '''Remove a publish subscription

        :param service: the service of the subscription to remove
        :type service: anything hash-able
        :param mask: the mask of the subscription to remove
        :type mask: int
        :param value: the value in the subscription to remove
        :type value: int

        :returns:
            a boolean indicating whether the subscription was there (True) and
            removed, or not (False)
        '''
        log.info("unsubscribing from publish %r" % (
                (service, (mask, value)),))
        return self._dispatcher.remove_local_subscription(
                const.MSG_TYPE_PUBLISH, service, mask, value)

    def publish(self, service, routing_id, method, args=None, kwargs=None,
            singular=False):
        '''Send a 1-way message

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param routing_id:
            the id used for routing within the registered handlers of the
            service
        :type routing_id: int
        :param method: the method name to call
        :type method: string
        :param args:
            The positional arguments to send along with the request. If the
            first positional argument is a generator object, the publish will
            be sent in chunks :ref:`(more info) <chunked-messages>`.
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict
        :param singular: if ``True``, only send the message to a single peer
        :type singular: bool

        :returns: None. use 'rpc' methods for requests with responses.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        if not self._dispatcher.send_publish(None, service, routing_id, method,
                args or (), kwargs or {}, singular=singular):
            raise errors.Unroutable()

    def publish_receiver_count(self, service, routing_id):
        '''Get the number of peers that would handle a particular publish

        :param service: the service name
        :type service: anything hash-able
        :param routing_id: the id used for limiting the service handlers
        :type routing_id: int
        '''
        peers = len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_PUBLISH, service, routing_id)))
        if self._dispatcher.locally_handles(const.MSG_TYPE_PUBLISH,
                service, routing_id):
            return peers + 1
        return peers

    def accept_rpc(self, service, mask, value, method, handler=None, schedule=True):
        '''Set a handler for incoming RPCs

        :param service: the incoming RPC must have this service
        :type service: anything hash-able
        :param mask:
            value to be bitwise-and'ed against the incoming id, the result of
            which must mask the 'value' param
        :type mask: int
        :param value:
            the result of `routing_id & mask` must match this in order to
            trigger the handler
        :type value: int
        :param method: the method name to trigger handler
        :type method: string
        :param handler:
            the function that will be called on incoming matching RPC requests
        :type handler: callable
        :param schedule:
            whether to schedule a separate greenlet running ``handler`` for
            each matching message. default ``True``.
        :type schedule: bool

        :raises:
            - :class:`ImpossibleSubscription
              <junction.errors.ImpossibleSubscription>` if there is no routing
              ID which could possibly match the mask/value pair
            - :class:`OverlappingSubscription
              <junction.errors.OverlappingSubscription>` if a prior rpc
              registration that overlaps with this one (there is a
              service/method/routing id that would match *both* this *and* a
              previously-made registration).
        '''
        # support @hub.accept_rpc(serv, mask, val, meth) decorator usage
        if handler is None:
            return lambda h: self.accept_rpc(
                    service, mask, value, method, h, schedule)

        log.info("accepting RPCs%s %r" % (
                " scheduled" if schedule else "",
                (service, (mask, value), method),))

        self._dispatcher.add_local_subscription(const.MSG_TYPE_RPC_REQUEST,
                service, mask, value, method, handler, schedule)

        return handler

    def unsubscribe_rpc(self, service, mask, value):
        '''Remove a rpc subscription

        :param service: the service of the subscription to remove
        :type service: anything hash-able
        :param mask: the mask of the subscription to remove
        :type mask: int
        :param value: the value in the subscription to remove
        :type value: int
        :param handler: the handler function of the subscription to remove
        :type handler: callable

        :returns:
            a boolean indicating whether the subscription was there (True) and
            removed, or not (False)
        '''
        log.info("unsubscribing from RPC %r" % ((service, (mask, value)),))
        return self._dispatcher.remove_local_subscription(
                const.MSG_TYPE_RPC_REQUEST, service, mask, value)

    def send_rpc(self, service, routing_id, method, args=None, kwargs=None,
            singular=False):
        '''Send out an RPC request

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
        :param method: the method name to call
        :type method: string
        :param args:
            The positional arguments to send along with the request. If the
            first argument is a generator, the request will be sent in chunks
            :ref:`(more info) <chunked-messages>`.
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict
        :param singular: if ``True``, only send the request to a single peer
        :type singular: bool

        :returns:
            a :class:`RPC <junction.futures.RPC>` object representing the
            RPC and its future response.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        rpc = self._dispatcher.send_rpc(service, routing_id, method,
                args or (), kwargs or {}, singular)

        if not rpc:
            raise errors.Unroutable()

        return rpc

    def wait_any(self, futures, timeout=None):
        '''Wait for the response for any (the first) of multiple futures

        This method will block until a response has been received.

        :param futures:
            a list made up of :class:`rpc <junction.futures.RPC>` and
            :class:`Dependent <junction.futures.Dependent>` objects
        :type futures: list
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :type timeout: float or None

        :returns:
            one of the :class:`RPC <junction.futures.RPC>`\s or
            :class:`Dependent <junction.futures.Dependent>`\s from ``futures``
            -- the first one to be completed (or any of the ones that were
            already completed) for which the ``completed`` attribute is
            ``True``.

        :raises:
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        return self._rpc_client.wait(futures, timeout)

    def rpc(self, service, routing_id, method, args=None, kwargs=None,
            timeout=None, singular=False):
        '''Send an RPC request and return the corresponding response

        This will block waiting until the response has been received.

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
        :param method: the method name to call
        :type method: string
        :param args:
            The positional arguments to send along with the request. If the
            first argument is a generator, the request will be sent in chunks
            :ref:`(more info) <chunked-messages>`.
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :type timeout: float or None
        :param singular: if ``True``, only send the request to a single peer
        :type singular: bool

        :returns:
            a list of the objects returned by the RPC's targets. these could be
            of any serializable type.

        :raises:
            - :class:`Unroutable <junction.errors.Unroutable>` if no peers are
              registered to receive the message
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        return self.send_rpc(service, routing_id, method,
                args or (), kwargs or {}, singular).wait(timeout)

    def rpc_receiver_count(self, service, routing_id):
        '''Get the number of peers that would handle a particular RPC

        :param service: the service name
        :type service: anything hash-able
        :param routing_id:
            the id used for narrowing within the service handlers
        :type routing_id: int

        :returns:
            the integer number of peers that would receive the described RPC
        '''
        peers = len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_RPC_REQUEST, service, routing_id)))
        if self._dispatcher.locally_handles(const.MSG_TYPE_RPC_REQUEST,
                service, routing_id):
            return peers + 1
        return peers

    def dependency_root(self, func):
        '''Create a parent-less :class:`Dependent <junction.futures.Dependent>`

        This is like :meth:`RPC.after <junction.futures.RPC.after>` or
        :meth:`Dependent.after <junction.futures.Dependent.after>` in that it
        wraps a callback with a new :class:`Dependent
        <junction.futures.Dependent>`, but this one has no parents (so the
        callback should take no arguments).

        :param func:
            the callback function for the dependent (this will be started
            immediately in a separate coroutine)
        :type func: callable

        :returns: a new :class:`Dependent <junction.futures.Dependent>`
        '''
        client = self._rpc_client
        dep = futures.Dependent(client, client.next_counter(), [], func)
        dep._complete()
        return dep

    def start(self):
        "Start up the hub's server, and have it start initiating connections"
        log.info("starting")

        self._listener_coro = backend.greenlet(self._listener)
        backend.schedule(self._listener_coro)

        for addr in self._peers:
            self.add_peer(addr)

    def add_peer(self, peer_addr):
        "Build a connection to the Hub at a given ``(host, port)`` address"
        peer = connection.Peer(
                self._ident, self._dispatcher, peer_addr, backend.Socket())
        peer.start()
        self._started_peers[peer_addr] = peer

    def _listener(self):
        server = backend.Socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server.bind(self.addr)
        server.listen(socket.SOMAXCONN)
        log.info("starting listener socket on %r" % (self.addr,))

        while not self._closing:
            try:
                client, addr = server.accept()
            except errors._BailOutOfListener:
                log.info("closing listener socket")
                server.close()
                break

            peer = connection.Peer(self._ident, self._dispatcher, addr, client,
                    initiator=False)
            peer.start()

            # we may block on the next accept() call for a while, and these
            # local vars will prevent the last peer from being garbage
            # collected if it goes down in the meantime.
            del client, peer
