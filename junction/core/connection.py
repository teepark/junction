from __future__ import absolute_import

import logging
import random
import socket
import struct

import mummy

from . import backend, const
from .. import errors


RECONNECT_JITTER = 0.25

log = logging.getLogger("junction.connection")


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
        self.send_queue = backend.Queue()
        self.established = backend.Event()
        self.reconnect_waiter = backend.Event()

        self._sender_coro = None
        self._receiver_coro = None

        # we'll get this from the peer on handshake
        self.ident = ()

    ##
    ## Public API
    ##

    def start(self):
        backend.schedule(self.starter_coro)

    def go_down(self, reconnect=False, expected=False):
        self.up = False
        self.established.clear()
        self.end_io_coros()
        subs = self.dispatcher.drop_peer(self)

        if not reconnect:
            self._closing = True
            self.reconnect_waiter.set()
            self.reconnect_waiter.clear()
            backend.schedule_in(1.0, self.sock.close)
        elif self.initiator and self.attempt_reconnects:
            self.schedule_restarter()

        if not expected:
            self.dispatcher.connection_lost(self, subs)

    def wait_connected(self, timeout=None):
        self.established.wait(timeout)
        return self.up

    def push(self, msg):
        self.send_queue.put(self.dump(msg))

    def push_string(self, msg):
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
        elif self.initiator and not self._closing and self.attempt_reconnects:
            self.schedule_restarter()
        else:
            self.up = False
            self.established.set()

    def restarter_coro(self):
        if self.reconnect():
            self.schedule_io_coros()

    def sender_coro(self):
        try:
            while 1:
                self.sock.sendall(self.send_queue.get())
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
        log.warn("connection to %r went down" % (self.ident,))
        self.go_down(reconnect=True, expected=False)

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
        self._sender_coro = backend.greenlet(self.sender_coro)
        backend.schedule(self._sender_coro)

        self._receiver_coro = backend.greenlet(self.receiver_coro)
        backend.schedule(self._receiver_coro)

    def end_io_coros(self):
        if self._sender_coro and backend.getcurrent() is not self._sender_coro:
            backend.end(self._sender_coro)

        if self._receiver_coro and \
                backend.getcurrent() is not self._receiver_coro:
            backend.end(self._receiver_coro)

    def schedule_restarter(self):
        backend.schedule(self.restarter_coro)

    def attempt_connect(self):
        log.info("attempting to connect to %r" % (self.target,))
        try:
            self.sock.connect(self.target)
        except socket.error:
            return False

        return self.attempt_handshake()

    def attempt_handshake(self):
        peername = self.sock.getpeername()
        log.info("sending a handshake to %r" % (peername,))

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
            log.warn("receiving handshake from %r failed" % (peername,))
            return False

        # validate the peer's handshake message format
        if (not received
                or not isinstance(received, tuple)
                or received[0] != const.MSG_TYPE_HANDSHAKE
                or len(received) != 2
                or not isinstance(received[1], tuple)
                or len(received[1]) != 2
                or not isinstance(received[1][0], (tuple, type(None)))
                or not isinstance(received[1][1], list)):
            log.warn("invalid handshake from %r" % (peername,))
            return False

        log.info("received handshake from %r" % (peername,))

        self.ident, subs = received[1]
        self.up = True
        self.established.set()

        if self.ident is None:
            # junction.Clients send None as their 'ident'
            return True
        return self.dispatcher.store_peer(self, subs)

    def pause_chain(self):
        # start with [0, 0.1], then double until we top out at
        # 30, but with each doubling include a jitter factor
        yield 0

        n = 0.1
        while 1:
            yield n
            # this arithmetic doubles it, and then modifies it by a random
            # ratio between -RECONNECT_JITTER and RECONNECT_JITTER (+-25%)
            n *= 2 * (1 - ((random.random() - 0.5) * 2 * RECONNECT_JITTER))
            if n > 30:
                break

        while 1:
            yield 30

    def reconnect(self):
        for pause in self.pause_chain():
            # reset
            self.sock.close()
            self.sock = backend.Socket()
            self.init_sock()
            self.established.clear()
            self.up = False

            self.dispatcher.add_reconnecting(self.addr, self)

            if not self.reconnect_waiter.wait(timeout=pause) or self._closing:
                return False

            if self.attempt_connect():
                return True

        return False

    def dump(self, msg):
        return dump(msg)

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


def dump(msg):
    msg = mummy.dumps(msg)
    return struct.pack("!I", len(msg)) + msg
