from __future__ import absolute_import

import socket
import time

from greenhouse import io, scheduler
from . import errors, futures
from .core import connection, const, dispatch, rpc


class Hub(object):
    'A hub in the server graph'
    def __init__(self, addr, peer_addrs, hostname=None):
        self.addr = addr
        self._ident = (hostname or addr[0], addr[1])
        self._peers = peer_addrs
        self._started_peers = {}
        self._closing = False
        self._listener_coro = None

        self._rpc_client = rpc.RPCClient()
        self._dispatcher = dispatch.Dispatcher(self._rpc_client)

    def wait_on_connections(self, conns=None, timeout=None):
        '''Wait for connections to be made and their handshakes to finish

        :param conns:
            a single or list of (host, port) tuples with the connections that
            must be finished before the method will return. defaults to all the
            peers the :class:`Hub` was instantiated with.
        :param timeout:
            maximum time to wait in seconds. with None, there is no timeout.
        :type timeout: float or None

        :returns:
            ``True`` if all connections were made, ``False`` if it hit the
            timeout instead.
        '''
        if timeout:
            deadline = time.time() + timeout
        conns = conns or self._peers
        if not hasattr(conns, "__iter__"):
            conns = [conns]

        for peer_addr in conns:
            remaining = max(0, deadline - time.time()) if timeout else None
            if not self._started_peers[peer_addr].wait_connected(remaining):
                return False
        return True

    def shutdown(self):
        'Close all peer connections and stop listening for new ones'
        for peer in self._dispatcher.peers.values():
            peer.go_down(reconnect=False)

        if self._listener_coro:
            scheduler.schedule_exception(
                    errors._BailOutOfListener(), self._listener_coro)

    def accept_publish(
            self, service, mask, value, method, handler, schedule=False):
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
        self._dispatcher.add_local_subscription(const.MSG_TYPE_PUBLISH,
                service, mask, value, method, handler, schedule)

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
        return self._dispatcher.remove_local_subscription(
                const.MSG_TYPE_PUBLISH, service, mask, value)

    def publish(self, service, routing_id, method, args, kwargs):
        '''Send a 1-way message

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
        :param method: the method name to call
        :type method: string
        :param args: the positional arguments to send along with the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict

        :returns: None. use 'rpc' methods for requests with responses.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        if not self._dispatcher.send_publish(
                service, routing_id, method, args, kwargs):
            raise errors.Unroutable()

    def publish_receiver_count(self, service, routing_id):
        '''Get the number of peers that would handle a particular publish

        :param service: the service name
        :type service: anything hash-able
        :param routing_id: the id used for limiting the service handlers
        :type routing_id: int
        '''
        return len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_PUBLISH, service, routing_id)))

    def accept_rpc(self, service, mask, value, method, handler, schedule=True):
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
        self._dispatcher.add_local_subscription(const.MSG_TYPE_RPC_REQUEST,
                service, mask, value, method, handler, schedule)

    def unsubscribe_rpc(self, service, mask, value, handler):
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
        return self._dispatcher.remove_local_subscription(
                const.MSG_TYPE_RPC_REQUEST, service, mask, value, handler)

    def send_rpc(self, service, routing_id, method, args, kwargs):
        '''Send out an RPC request

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
        :param method: the method name to call
        :type method: string
        :param args: the positional arguments to send along with the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict

        :returns:
            a :class:`RPC <junction.futures.RPC>` object representing the
            RPC and its future response.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        rpc = self._dispatcher.send_rpc(
                service, routing_id, method, args, kwargs)

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

    def rpc(self, service, routing_id, method, args, kwargs, timeout=None):
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
        :param args: the positional arguments to send along with the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :type timeout: float or None

        :returns:
            a list of the objects returned by the RPC's targets. these could be
            of any serializable type.

        :raises:
            - :class:`Unroutable <junction.errors.Unroutable>` if no peers are
              registered to receive the message
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        return self.send_rpc(
                service, routing_id, method, args, kwargs).wait(timeout)

    def rpc_receiver_count(self, service, routing_id, method):
        '''Get the number of peers that would handle a particular RPC

        :param service: the service name
        :type service: anything hash-able
        :param routing_id:
            the id used for narrowing within the service handlers
        :type routing_id: int
        :param method: the method name
        :type method: string

        :returns:
            the integer number of peers that would receive the described RPC
        '''
        return len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)))

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
        self._listener_coro = scheduler.greenlet(self._listener)
        scheduler.schedule(self._listener_coro)

        for addr in self._peers:
            peer = connection.Peer(
                    self._ident, self._dispatcher, addr, io.Socket())
            peer.start()
            self._started_peers[addr] = peer

    def _listener(self):
        server = io.Socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server.bind(self.addr)
        server.listen(socket.SOMAXCONN)

        try:
            while not self._closing:
                client, addr = server.accept()
                peer = connection.Peer(self._ident, self._dispatcher, addr, client,
                        initiator=False)
                peer.start()

                # we may block on the accept() call for a while, and these local
                # vars will prevent the last peer from being garbage collected if
                # it goes down in the meantime.
                del client, peer
        except errors._BailOutOfListener:
            server.close()
