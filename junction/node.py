from __future__ import absolute_import

import socket
import time

from greenhouse import io, scheduler
from . import errors
from .core import connection, const, dispatch, rpc


class Node(object):
    'A node in the server graph'
    def __init__(self, addr, peer_addrs, hostname=None):
        self.addr = addr
        self._ident = (hostname or addr[0], addr[1])
        self._peers = peer_addrs
        self._started_peers = {}
        self._closing = False
        self._listener_coro = None

        self._rpc_client = rpc.RPCClient()
        self._dispatcher = dispatch.Dispatcher(self.VERSION, self._rpc_client)

    def wait_on_connections(self, conns=None, timeout=None):
        '''Wait for connections to be made and their handshakes to finish

        :param conns:
            a single or list of (host, port) tuples with the connections that
            must be finished before the method will return. defaults to all the
            peers the :class:`Node` was instantiated with.
        :param timeout:
            maximum time to wait in seconds. with None, there is no timeout.
        :type timeout: float or None

        :returns:
            ``True`` if it timed out or connects or handshakes failed,
            otherwise ``False``
        '''
        if timeout:
            deadline = time.time() + timeout
        conns = conns or self._peers
        if not hasattr(conns, "__iter__"):
            conns = [conns]

        for peer_addr in conns:
            remaining = max(0, deadline - time.time()) if timeout else None
            peer = self._started_peers[peer_addr]
            if peer.wait_connected():
                return True

        return False

    def shutdown(self):
        'Close all peer connections and stop listening for new ones'
        for peer in self._dispatcher.peers.values():
            peer.go_down(reconnect=False)

        if self._listener_coro:
            scheduler.schedule_exception(
                    errors._BailOutOfListener(), self._listener_coro)

    def accept_publish(
            self, service, method, mask, value, handler, schedule=False):
        '''Set a handler for incoming publish messages

        :param service: the incoming message must have this service
        :type service: anything hash-able
        :param method: the method name to trigger handler
        :type method: string
        :param mask:
            value to be bitwise-and'ed against the incoming id, the result of
            which must mask the 'value' param
        :type mask: int
        :param value:
            the result of `routing_id & mask` must match this in order to
            trigger the handler
        :type value: int
        :param handler:
            the function that will be called on incoming matching messages
        :type handler: callable
        :param schedule:
            whether to schedule a separate greenlet running ``handler`` for
            each matching message. default ``False``.
        :type schedule: bool

        :returns:
            a boolean indicating whether a new subscription was stored. this
            can come back ``False`` if the subscription is somehow invalid (the
            mask/value pair could never match anything, or it overlaps with
            an existing subscription)
        '''
        return bool(self._dispatcher.add_local_subscriptions(handler,
            [(const.MSG_TYPE_PUBLISH, service, method, mask, value, schedule)]))

    def unsubscribe_publish(self, service, method, mask, value, handler):
        '''Remove a publish subscription

        :param service: the service of the subscription to remove
        :type service: anything hash-able
        :param method: the method in the subscription
        :type method: string
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
                const.MSG_TYPE_PUBLISH, service, method, mask, value, handler)

    def publish(self, service, method, routing_id, args, kwargs):
        '''Send a 1-way message

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param method: the method name to call
        :type method: string
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
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
                service, method, routing_id, args, kwargs):
            raise errors.Unroutable()

    def publish_receiver_count(self, service, method, routing_id):
        '''Get the number of peers that would handle a particular publish

        :param service: the service name
        :type service: anything hash-able
        :param method: the method name
        :type method: string
        :param routing_id:
            the id used for narrowing within the (service, method) handlers
        :type routing_id: int
        '''
        return len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_PUBLISH, service, method, routing_id)))

    def accept_rpc(self, service, method, mask, value, handler, schedule=True):
        '''Set a handler for incoming RPCs

        :param service: the incoming RPC must have this service
        :type service: anything hash-able
        :param method: the method name to trigger handler
        :type method: string
        :param mask:
            value to be bitwise-and'ed against the incoming id, the result of
            which must mask the 'value' param
        :type mask: int
        :param value:
            the result of `routing_id & mask` must match this in order to
            trigger the handler
        :type value: int
        :param handler:
            the function that will be called on incoming matching RPC requests
        :type handler: callable
        :param schedule:
            whether to schedule a separate greenlet running ``handler`` for
            each matching message. default ``True``.
        :type schedule: bool

        :returns:
            a boolean indicating whether a new subscription was stored. this
            can come back ``False`` if the subscription is somehow invalid (the
            mask/value pair could never match anything, or it overlaps with
            an existing subscription)
        '''
        return bool(self._dispatcher.add_local_subscriptions(
            handler,
            [(const.MSG_TYPE_RPC_REQUEST,
                service,
                method,
                mask,
                value,
                schedule)]))

    def unsubscribe_rpc(self, service, method, mask, value, handler):
        '''Remove a rpc subscription

        :param service: the service of the subscription to remove
        :type service: anything hash-able
        :param method: the method in the subscription
        :type method: string
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
                const.MSG_TYPE_RPC_REQUEST, service, method, mask, value,
                handler)

    def send_rpc(self, service, method, routing_id, args, kwargs):
        '''Send out an RPC request

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param method: the method name to call
        :type method: string
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
        :param args: the positional arguments to send along with the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict

        :returns:
            a :class:`RPC <junction.core.rpc.RPC>` object representing the
            RPC and its future response.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        rpc = self._dispatcher.send_rpc(
                service, method, routing_id, args, kwargs)

        if not rpc:
            raise errors.Unroutable()

        return rpc

    def wait_any(self, futures, timeout=None):
        '''Wait for the response for any (the first) of multiple futures

        This method will block until a response has been received.

        :param futures:
            a list made up of :class:`rpc <junction.core.rpc.RPC>` and
            :class:`Dependent <junction.core.rpc.Dependent>` objects
        :type futures: list
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :type timeout: float or None

        :returns:
            one of the :class:`RPC <junction.core.rpc.RPC>`\s or
            :class:`Dependent <junction.core.rpc.Dependent>`\s from ``futures``
            -- the first one to be completed (or any of the ones that were
            already completed) for which the ``completed`` attribute is
            ``True``.

        :raises:
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        return self._rpc_client.wait(futures, timeout)

    def rpc(self, service, method, routing_id, args, kwargs, timeout=None):
        '''Send an RPC request and return the corresponding response

        This will block waiting until the response has been received.

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param method: the method name to call
        :type method: string
        :param routing_id:
            The id used for routing within the registered handlers of the
            service.
        :type routing_id: int
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
                service, method, routing_id, args, kwargs).wait(timeout)

    def rpc_receiver_count(self, service, method, routing_id):
        '''Get the number of peers that would handle a particular RPC

        :param service: the service name
        :type service: anything hash-able
        :param method: the method name
        :type method: string
        :param routing_id:
            the id used for narrowing within the (service, method) handlers
        :type routing_id: int
        '''
        return len(list(self._dispatcher.find_peer_routes(
            const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)))

    def start(self):
        "Start up the node's server, and have it start initiating connections"
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
