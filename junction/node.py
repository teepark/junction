from __future__ import absolute_import

import socket

from greenhouse import io, scheduler
from . import connection, const, dispatch, errors, rpc


class Node(object):
    def __init__(self, addr, peer_addrs):
        self.addr = addr
        self._peers = peer_addrs
        self._closing = False

        self._dispatcher = dispatch.Dispatcher(addr, self.VERSION)
        self._rpc_client = rpc.Client(self._dispatcher)
        self._dispatcher.rpc_client = self._rpc_client

    def start(self):
        scheduler.schedule(self.listener_coro)
        map(self.create_connection, self._peers)

    def create_connection(self, addr):
        peer = connection.Peer(self._dispatcher, addr, io.Socket())
        peer.start(connect=True)

    def listener_coro(self):
        server = io.Socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server.bind(self.addr)
        server.listen(socket.SOMAXCONN)

        while not self._closing:
            client, addr = server.accept()
            peer = connection.Peer(self._dispatcher, addr, client, connected=1)
            peer.start(connect=False)

    def wait_on_connections(self, conns=None):
        conns = conns or self._peers
        if not hasattr(conns, "__iter__"):
            conns = [conns]
        for peer_addr in conns:
            self._dispatcher.all_peers[peer_addr].established.wait()

    def accept_publish(self, service, method, mask, value, handler):
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

        :returns:
            a boolean indicating whether a new registration was stored. this
            can come back ``False`` if the registration is somehow invalid (the
            mask/value pair could never match anything, or it overlaps with
            an existing registration)
        '''
        added = self._dispatcher.add_local_regs(handler,
            [(const.SERVICE_TYPE_PUBLISH, service, method, mask, value)])

        if not added:
            return False

        msg = (const.MSG_TYPE_ANNOUNCE,
                [(const.SERVICE_TYPE_PUBLISH, service, method, mask, value)])

        for peer in self._dispatcher.peers():
            if peer.established.is_set():
                peer.send_queue.put(msg)

        return True

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
        :param args:
            the positional arguments (besides routing_id) to send along with
            the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict

        :returns: None. use 'rpc' methods for requests with responses.

        :raises: Unroutable if no peers are registered to receive the message
        '''
        msg = (const.MSG_TYPE_PUBLISH,
                (service, method, routing_id, args, kwargs))

        found_one = False
        for peer in self._dispatcher.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, method, routing_id):
            found_one = True
            peer.send_queue.put(msg)

        if not found_one:
            raise errors.Unroutable()

    def accept_rpc(self, service, method, mask, value, handler):
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

        :returns:
            a boolean indicating whether a new registration was stored. this
            can come back ``False`` if the registration is somehow invalid (the
            mask/value pair could never match anything, or it overlaps with
            an existing registration)
        '''
        added = self._dispatcher.add_local_regs(handler,
            [(const.SERVICE_TYPE_RPC, service, method, mask, value)])

        if not added:
            return False

        msg = (const.MSG_TYPE_ANNOUNCE,
                [(const.SERVICE_TYPE_RPC, service, method, mask, value)])

        for peer in self._dispatcher.peers():
            if peer.established.is_set():
                peer.send_queue.put(msg)

        return True

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
        :param args:
            the positional arguments (besides routing_id) to send along with
            the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict

        :returns:
            an integer counter that can be used to retrieve the RPC response
            (with :meth:`wait_rpc`)
        '''
        counter = self._rpc_client.request(
                service, method, routing_id, args, kwargs)

        if not counter:
            raise errors.Unroutable()

        return counter

    def wait_rpc(self, counter, timeout=None):
        '''Wait for and return a given RPC's response

        This method will block until the response has been received.

        :param counter: an id returned by :meth:`send_rpc`
        :type counter: int
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :tupe timeout: float or None

        :returns:
            a list of the objects returned by the remote RPC's targets. these
            could be of any serializable type. one or more items in the list
            may be exceptions, in which case they describe failures in the
            peers.

        :raises:
            RPCWaitTimeout if a timeout was provided and it expires
        '''
        results = []

        rpc_client_results = self._rpc_client.wait(counter, timeout)

        if rpc_client_results is None:
            raise ValueError("counter doesn't correspond to an in-flight RPC")

        for peer_ident, rc, result in rpc_client_results:
            if not rc:
                results.append(result)
                continue

            if rc == const.RPC_ERR_NOHANDLER:
                results.append(errors.NoRemoteHandler(
                            "RPC mistakenly sent to %r" % (peer_ident,)))
            elif rc == const.RPC_ERR_KNOWN:
                err_code, err_args = result
                results.append(errors.HANDLED_ERROR_TYPES.get(
                    err_code, errors.HandledError)(peer_ident, *err_args))
            elif rc == const.RPC_ERR_UNKNOWN:
                results.append(errors.RemoteException(peer_ident, result))
            else:
                results.append(errors.UnrecognizedRemoteProblem(
                    peer_ident, rc, result))

        return results

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
        :param args:
            the positional arguments (besides routing_id) to send along with
            the request
        :type args: tuple
        :param kwargs: keyword arguments to send along with the request
        :type kwargs: dict
        :param timeout:
            maximum time to wait for a response in seconds. with None, there is
            no timeout.
        :tupe timeout: float or None

        :returns:
            the object returned by the remote RPC target. this could be of any
            serializable type.

        :raises:
            RPCWaitTimeout if a timeout was provided and it expires
        '''
        return self.wait_rpc(
                self.send_rpc(service, method, routing_id, args, kwargs),
                timeout)
