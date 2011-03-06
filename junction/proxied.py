from __future__ import absolute_import

from greenhouse import io
from . import connection, dispatch, errors, rpc


class Client(object):
    def __init__(self, node_address):
        self.node_addr = node_address
        self._rpc_client = rpc.ProxiedClient()
        self._dispatcher = dispatch.Dispatcher(self.VERSION, self._rpc_client)
        self._peer = None

    def connect(self):
        "Initiate the connection to a proxying node"
        self._peer = connection.Peer(
                None, self._dispatcher, self.node_addr, io.Socket())
        self._peer.start()

    def wait_on_connections(self, timeout=None):
        '''Wait for connections to be made and their handshakes to finish

        :param timeout:
            maximum time to wait in seconds. with None, there is no timeout.
        :type timeout: float or None

        :returns:
            ``True`` if it timed out or if the connect or handshake failed,
            otherwise ``False``
        '''
        return self._peer.wait_connected(timeout)

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
            :class:`Unroutable <junction.errors.Unroutable>` if the client
            doesn't have a connection to a node
        '''
        if not self._peer.up:
            raise Unroutable()

        self._dispatcher.send_proxied_publish(
                service, method, routing_id, args, kwargs)

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
            a :class:`RPC <junction.rpc.RPC>` object representing the
            RPC and its future response.

        :raises:
            :class:`Unroutable <junction.errors.Unroutable>` if the client
            doesn't have a connection to a node
        '''
        if not self._peer.up:
            raise Unroutable()

        return self._rpc_client.request([self._peer],
                service, method, routing_id, args, kwargs)

    def wait_any_rpc(self, rpcs, timeout=None):
        '''Wait for the response for any (the first) of multiple RPCs

        This method will block until a response has been received.

        :param rpcs:
            a list of rpc :class:`rpc <junction.rpc.RPC>` objects (as
            returned by :meth:`send_rpc`)
        :type rpcs: list of :class:`RPCs <junction.rpc.Response>`
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :type timeout: float or None

        :returns:
            one of the :class:`RPC <junction.rpc.RPC>` s from
            ``rpcs`` -- the first one to be completed (or any of the ones
            that were already completed) for which the ``completed`` attribute
            is ``True``.

        :raises:
            - :class:`RPCWaitTimeout <junction.errors.RPCWaitTimeout>` if a
              timeout was provided and it expires
        '''
        return self._rpc_client.wait(rpcs, timeout)

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
            - :class:`RPCWaitTimeout <junction.errors.RPCWaitTimeout>` if a
              timeout was provided and it expires
        '''
        rpc = self.send_rpc(service, method, routing_id, args, kwargs)
        results = rpc.wait(timeout)
        if not rpc.target_count:
            raise errors.Unroutable()
        return results
