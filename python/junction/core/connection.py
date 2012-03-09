from __future__ import absolute_import

import itertools
import socket
import struct

from greenhouse import io, scheduler, util
import mummy

from . import const
from .. import errors


class Peer(object):
    def __init__(self, local_addr, dispatcher, addr, sock, initiator=True,
            reconnect=True):
        self.local_addr = local_addr
        self.dispatcher = dispatcher
        self.addr = addr
        self.sock = sock
        self.initiator = initiator
        self.up = False
        self._closing = False

        self.attempt_reconnects = reconnect
        self.reconnect_pauses = [0] + [(2 ** i) / 10.0 for i in xrange(9)]
        self.send_queue = util.Queue()
        self.established = util.Event()
        self.reconnect_waiter = util.Event()

        self._sender_coro = None
        self._receiver_coro = None

        # we'll get these from the peer on handshake
        self.ident = ()
        self.subscriptions = []

    ##
    ## Public API
    ##

    def start(self):
        scheduler.schedule(self.starter_coro)

    def go_down(self, reconnect=False):
        self.up = False
        self.established.clear()
        self.end_io_coros()
        self.dispatcher.drop_peer(self)

        if not reconnect:
            self._closing = True
            self.reconnect_waiter.set()
            self.reconnect_waiter.clear()
            scheduler.schedule_in(1.0, self.sock.close)
        elif self.initiator:
            self.schedule_restarter()

    def wait_connected(self, timeout=None):
        self.established.wait(timeout)
        return not self.up

    def push(self, msg):
        self.send_queue.put(msg)

    ##
    ## Coroutines
    ##

    def starter_coro(self):
        self.init_sock()

        if self.initiator:
            success = self.attempt_connect()
        else:
            success = self.attempt_handshake()

        if success:
            self.schedule_io_coros()
        elif self.initiator and not self._closing:
            self.schedule_restarter()

    def restarter_coro(self):
        if self.reconnect():
            self.schedule_io_coros()
        else:
            self.established.set()

    def sender_coro(self):
        try:
            while 1:
                msg = self.send_queue.get()
                if msg is _END:
                    break

                self.sock.sendall(self.dump(msg))
        except socket.error:
            self.connection_failure()

    def receiver_coro(self):
        try:
            while 1:
                self.dispatcher.incoming(self, self.recv_one())
        except (socket.error, errors.MessageCutOff):
            self.connection_failure()

    ##
    ## Utilities
    ##

    @property
    def target(self):
        return self.ident or self.addr

    def connection_failure(self):
        self.go_down(reconnect=True)

    def init_sock(self):
        # disable Nagle algorithm with the NODELAY option
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

        # activate TCP keepalive and tweak the settings. the strategy is:
        # after 30 seconds of inactivity, send up to 6 probes, 5 seconds apart,
        # so dead connections are detected ~1 minute after their last activity.
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 30)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 6)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)

    def schedule_io_coros(self):
        self._sender_coro = scheduler.greenlet(self.sender_coro)
        scheduler.schedule(self._sender_coro)

        self._receiver_coro = scheduler.greenlet(self.receiver_coro)
        scheduler.schedule(self._receiver_coro)

    def end_io_coros(self):
        if self._sender_coro:
            scheduler.end(self._sender_coro)

        if self._receiver_coro:
            scheduler.end(self._receiver_coro)

    def schedule_restarter(self):
        scheduler.schedule(self.restarter_coro)

    def attempt_connect(self):
        try:
            self.sock.connect(self.target)
        except socket.error:
            return False

        return self.attempt_handshake()

    def attempt_handshake(self):
        # send a handshake message
        try:
            self.sock.sendall(self.dump((const.MSG_TYPE_HANDSHAKE, (
                self.local_addr,
                list(self.dispatcher.local_subscriptions())))))
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
                or len(received[1]) != 2
                or not isinstance(received[1][0], (tuple, type(None)))
                or not isinstance(received[1][1], list)):
            return False

        self.ident, self.subscriptions = received[1]
        self.up = True
        self.established.set()

        return self.ident is None or self.dispatcher.store_peer(self)

    def reconnect(self):
        if not self.attempt_reconnects:
            return False

        pauses = itertools.chain(self.reconnect_pauses, itertools.repeat(30.0))

        for pause in pauses:
            # reset
            self.sock.close()
            self.sock = io.Socket()
            self.init_sock()
            self.established.clear()
            self.up = False

            if not self.reconnect_waiter.wait(timeout=pause) or self._closing:
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


def compare(peerA, peerB):
    if not peerB.up:
        return peerA, peerB
    if not peerA.up:
        return peerB, peerA

    if (peerA.ident < peerA.local_addr) == peerA.initiator:
        return peerA, peerB
    return peerB, peerA


_END = object()
