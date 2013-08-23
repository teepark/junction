from __future__ import absolute_import

import logging
import random
import socket
import time

import mummy

from . import errors, futures
from .core import backend, connection, const, dispatch, rpc


log = logging.getLogger("junction.hub")


#  65535 byte IP packet (largest representable in the 2 byte length header)
#  -  20 byte IP header
#  -   8 byte UDP header
#  _____
MAX_UDP_PACKET_SIZE = 65507


class Hub(object):
    'A hub in the server graph'
    def __init__(self, addr, peer_addrs, hostname=None, hooks=None):
        self.addr = addr
        self._ident = (hostname or addr[0], addr[1])
        self._peers = peer_addrs
        self._started_peers = {}
        self._closing = False
        self._listener_coro = None
        self._udp_listener_coro = None

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
                    log.warn("connect wait timed out after %.2f seconds" %
                            timeout)
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
        if self._udp_listener_coro:
            backend.schedule_exception(
                    errors._BailOutOfListener(), self._udp_listener_coro)

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
            singular=False, udp=False):
        '''Send a 1-way message

        :param service: the service name (the routing top level)
        :type service: anything hash-able
        :param int routing_id:
            the id used for routing within the registered handlers of the
            service
        :param string method: the method name to call
        :param tuple args:
            The positional arguments to send along with the request. If the
            first positional argument is a generator object, the publish will
            be sent in chunks :ref:`(more info) <chunked-messages>`.
        :param dict kwargs: keyword arguments to send along with the request
        :param bool singular: if ``True``, only send the message to a single peer
        :param bool udp: deliver the message over UDP instead of the usual TCP

        :returns: None. use 'rpc' methods for requests with responses.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if no peers are
            registered to receive the message
        '''
        if udp:
            func = self._dispatcher.send_publish_udp
        else:
            func = self._dispatcher.send_publish
        if not func(None, service, routing_id, method,
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

    def accept_rpc(self, service, mask, value, method,
            handler=None, schedule=True):
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
        rpc = self.send_rpc(service, routing_id, method,
                args or (), kwargs or {}, singular)
        rpc.wait(timeout)
        return rpc.value

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

    def start(self):
        "Start up the hub's server, and have it start initiating connections"
        log.info("starting")

        self._listener_coro = backend.greenlet(self._listener)
        self._udp_listener_coro = backend.greenlet(self._udp_listener)
        backend.schedule(self._listener_coro)
        backend.schedule(self._udp_listener_coro)

        for addr in self._peers:
            self.add_peer(addr)

    def add_peer(self, peer_addr):
        "Build a connection to the Hub at a given ``(host, port)`` address"
        peer = connection.Peer(
                self._ident, self._dispatcher, peer_addr, backend.Socket())
        peer.start()
        self._started_peers[peer_addr] = peer

    @property
    def peers(self):
        "list of the (host, port) pairs of all connected peer Hubs"
        return [addr for (addr, peer) in self._dispatcher.peers.items()
                if peer.up]

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

    def _udp_listener(self):
        sock = backend.Socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.addr)

        log.info("starting UDP listener socket on %r" % (self.addr,))

        while not self._closing:
            try:
                msg, addr = sock.recvfrom(MAX_UDP_PACKET_SIZE)
            except errors._BailOutOfListener:
                log.info("closing listener socket")
                server.close()
                break

            msg = mummy.loads(msg)
            if not isinstance(msg, tuple) or len(msg) != 3:
                log.warn("malformed UDP message sent from %r" % (addr,))
            msg_type, sender_hostport, msg = msg
            if msg_type not in const.UDP_ALLOWED:
                log.warn("disallowed UDP message type %r from %r" %
                        (msg_type, sender_hostport))
                continue

            peer = self._dispatcher.peers[sender_hostport]
            self._dispatcher.incoming(peer, (msg_type, msg))
