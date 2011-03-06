from __future__ import absolute_import

import socket
import struct

from greenhouse import io, scheduler, utils
import mummy

from . import const, errors


class Peer(object):
    def __init__(self, local_addr, dispatcher, addr, sock, connect=True):
        self.local_addr = local_addr
        self.dispatcher = dispatcher
        self.addr = addr
        self.sock = sock

        self._closing = False
        self._started = False
        self.initiator = connect
        self.send_queue = utils.Queue()
        self.established = utils.Event()

        self.reconnect_pauses = [(i ** 2) / 2.0 for i in xrange(10)]

        self._sender_coro = None
        self._receiver_coro = None
        self.up = False

        # we'll get these from the peer on handshake
        self.ident = ()
        self.version = ()
        self.subscriptions = []

    def start(self):
        if self._started:
            return
        self._started = True

        scheduler.schedule(self.starter_coro)

    def wait_connected(self, timeout=None):
        if self.established.wait(timeout):
            return True
        return not self.up

    def starter_coro(self):
        self.init_sock()

        attempt = (self.attempt_connect if self.initiator
                else self.attempt_handshake)
        if attempt():
            self.schedule_coros()
        else:
            self.connection_restarter()

    def init_sock(self):
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def reset(self):
        self.sock.close()
        self.sock = io.Socket()
        self.init_sock()
        self.established.clear()
        self.up = False

    def attempt_handshake(self):
        # send a handshake message
        try:
            self.sock.sendall(self.dump((const.MSG_TYPE_HANDSHAKE, (
                    self.local_addr,
                    self.dispatcher.version,
                    list(self.dispatcher.local_registrations())))))
        except socket.error:
            return False

        # receive the peer's handshake message
        try:
            received = self.recv_one()
        except (socket.error, errors.MessageCutOff):
            return False

        # validate the peer's handshake message format
        if (not received
                or not isinstance(received, tuple)
                or received[0] != const.MSG_TYPE_HANDSHAKE
                or len(received) != 2
                or len(received[1]) != 3):
            return False

        self.ident, self.version, self.subscriptions = received[1]
        self.up = True
        self.established.set()

        if not self.dispatcher.store_peer(self):
            self.up = False
            return False

        return True

    def attempt_connect(self):
        try:
            self.sock.connect(self.addr)
        except socket.error:
            return False

        return self.attempt_handshake()

    def reconnect(self):
        for pause in self.reconnect_pauses:
            self.reset()

            scheduler.pause_for(pause)
            if self._closing:
                return False

            if self.attempt_connect():
                return True

        return False

    def dump(self, msg):
        msg = mummy.dumps(msg)
        return struct.pack("!I", len(msg)) + msg

    def read_bytes(self, count):
        data = [self.sock.recv(count)]
        count -= len(data[0])
        while count:
            data.append(self.sock.recv(count))
            if not data[-1]:
                raise errors.MessageCutOff(sum(map(len, data)))
            count -= len(data[-1])
        return ''.join(data)

    def recv_one(self):
        size = struct.unpack("!I", self.read_bytes(4))[0]
        return mummy.loads(self.read_bytes(size))

    def sender_coro(self):
        try:
            while not self._closing:
                msg = self.send_queue.get()
                if msg is _END:
                    break
                msg = self.dump(msg)
                self.sock.sendall(msg)
        except socket.error:
            self.connection_failure()

    def receiver_coro(self):
        while not self._closing:
            try:
                msg = self.recv_one()
                self.dispatcher.incoming(self, msg)
            except (socket.error, errors.MessageCutOff):
                self.connection_failure()
                break

    def unschedule_coros(self):
        if self._sender_coro is not None:
            scheduler.end(self._sender_coro)
        if self._receiver_coro is not None:
            scheduler.end(self._receiver_coro)

    def schedule_coros(self):
        self._sender_coro = scheduler.greenlet(self.sender_coro)
        self._receiver_coro = scheduler.greenlet(self.receiver_coro)

        scheduler.schedule(self._sender_coro)
        scheduler.schedule(self._receiver_coro)

    def connection_failure(self):
        self.up = False
        self.dispatcher.drop_peer(self)
        self.unschedule_coros()
        if self.ident is not None:
            scheduler.schedule(self.connection_restarter)

    def connection_restarter(self):
        if not self._closing and self.reconnect():
            self.schedule_coros()

    def shutdown(self):
        self._closing = True
        self.sock.close()
        self.send_queue.put(_END)


_END = object()
