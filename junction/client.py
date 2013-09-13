from __future__ import absolute_import

import collections
import logging
import weakref

from . import errors, futures
from .core import backend, connection, const, dispatch, rpc


log = logging.getLogger("junction.client")


class Client(object):
    "A junction client without the server"
    def __init__(self, addrs):
        self._rpc_client = rpc.ProxiedClient(self)
        self._dispatcher = dispatch.Dispatcher(self._rpc_client, None)
        self._peer = None

        # allow just a single (host, port) pair
        if (isinstance(addrs, tuple) and
                len(addrs) == 2 and
                isinstance(addrs[0], str) and
                isinstance(addrs[1], int)):
            addrs = [addrs]
        self._addrs = collections.deque(addrs)

    def connect(self):
        "Initiate the connection to a proxying hub"
        log.info("connecting")

        # don't have the connection attempt reconnects, because when it goes
        # down we are going to cycle to the next potential peer from the Client
        self._peer = connection.Peer(
                None, self._dispatcher, self._addrs.popleft(),
                backend.Socket(), reconnect=False)
        self._peer.start()

    def wait_connected(self, timeout=None):
        '''Wait for connections to be made and their handshakes to finish

        :param timeout:
            maximum time to wait in seconds. with None, there is no timeout.
        :type timeout: float or None

        :returns:
            ``True`` if all connections were made, ``False`` if one or more
            failed.
        '''
        result = self._peer.wait_connected(timeout)
        if not result:
            if timeout is not None:
                log.warn("connect wait timed out after %.2f seconds" % timeout)
        return result

    def reset(self):
        "Close the current failed connection and prepare for a new one"
        log.info("resetting client")
        rpc_client = self._rpc_client
        self._addrs.append(self._peer.addr)
        self.__init__(self._addrs)
        self._rpc_client = rpc_client
        self._dispatcher.rpc_client = rpc_client
        rpc_client._client = weakref.ref(self)

    def shutdown(self):
        'Close the hub connection'
        log.info("shutting down")
        self._peer.go_down(reconnect=False, expected=True)

    def publish(self, service, routing_id, method, args=None, kwargs=None,
            broadcast=False):
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
        :param broadcast:
            if ``True``, send to every peer with a matching subscription
        :type broadcast: bool

        :returns: None. use 'rpc' methods for requests with responses.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if the client
            doesn't have a connection to a hub
        '''
        if not self._peer.up:
            raise errors.Unroutable()

        self._dispatcher.send_proxied_publish(service, routing_id, method,
                args or (), kwargs or {}, singular=not broadcast)

    def publish_receiver_count(
            self, service, routing_id, method, timeout=None):
        '''Get the number of peers that would handle a particular publish

        This method will block until a response arrives

        :param service: the service name
        :type service: anything hash-able
        :param routing_id:
            the id used for narrowing within the service handlers
        :type routing_id: int
        :param method: the method name
        :type method: string
        :param timeout: maximum time to wait for the response
        :type timeout: int, float or None

        :raises:
            - :class:`Unroutable <junction.errors.Unroutable>` if no peers are
              registered to receive the message
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        if not self._peer.up:
            raise errors.Unroutable()

        return self._rpc_client.recipient_count(self._peer,
                const.MSG_TYPE_PUBLISH, service, routing_id, method).wait(
                        timeout)[0]

    def send_rpc(self, service, routing_id, method, args=None, kwargs=None,
            broadcast=False):
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
        :param broadcast:
            if ``True``, send to all peers with matching subscriptions
        :type broadcast: bool

        :returns:
            a :class:`RPC <junction.futures.RPC>` object representing the
            RPC and its future response.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if the client
            doesn't have a connection to a hub
        '''
        if not self._peer.up:
            raise errors.Unroutable()

        return self._dispatcher.send_proxied_rpc(service, routing_id, method,
                args or (), kwargs or {}, not broadcast)

    def rpc(self, service, routing_id, method, args=None, kwargs=None,
            timeout=None, broadcast=False):
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
        :param broadcast:
            if ``True``, send to all peers with matching subscriptions
        :type broadcast: bool

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
                args or (), kwargs or {}, broadcast=broadcast)
        return rpc.get(timeout)

    def rpc_receiver_count(self, service, routing_id, method, timeout=None):
        '''Get the number of peers that would handle a particular RPC

        This method will block until a response arrives

        :param service: the service name
        :type service: anything hash-able
        :param routing_id:
            the id used for narrowing within the service handlers
        :type routing_id: int
        :param method: the method name
        :type method: string

        :returns:
            the integer number of peers that would receive the described RPC

        :raises:
            - :class:`Unroutable <junction.errors.Unroutable>` if no peers are
              registered to receive the message
            - :class:`WaitTimeout <junction.errors.WaitTimeout>` if a timeout
              was provided and it expires
        '''
        if not self._peer.up:
            raise errors.Unroutable()

        return self._rpc_client.recipient_count(self._peer,
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method).wait(
                        timeout)[0]
