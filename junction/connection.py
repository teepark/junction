from __future__ import absolute_import

import socket
import struct

from greenhouse import scheduler, utils
import mummy

from . import const, errors


class Peer(object):
    def __init__(self, local_addr, dispatcher, addr, sock, connect=True):
        self.local_addr = local_addr
        self.dispatcher = dispatcher
        self.addr = addr
        self.sock = sock

        self._sock_inited = False
        self._closing = False
        self._connect_failed = False
        self._establish_failed = False
        self.connected = utils.Event()
        self.initiator = connect
        if not connect:
            self.connected.set()
        self.established = utils.Event()
        self.send_queue = utils.Queue()

        # will get these from the handshake
        self.ident = ()
        self.version = ()
        self.regs = []

    def init_sock(self):
        if self._sock_inited:
            return
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._sock_inited = True

    def connect(self):
        try:
            self.sock.connect(self.addr)
        except socket.error:
            self._connect_failed = True
        self.connected.set()

    def establish(self):
        self.connected.wait()
        if self._connect_failed:
            self._establish_failed = True
            self.established.set()
            return

        # send a message providing information about ourself
        try:
            self.sock.sendall(self.dump((const.MSG_TYPE_HANDSHAKE, (
                    self.local_addr,
                    self.dispatcher.version,
                    list(self.dispatcher.local_registrations())))))
        except socket.error:
            self._establish_failed = True
            self.established.set()
            return

        # expect to get a similar message back from the peer
        try:
            received = self.recv_one()
        except socket.error, exc:
            return
        except errors.MessageCutOff, exc:
            self._establish_failed = True
            self.established.set()
            if not exc.args[0]:
                return
            raise

        if not isinstance(received, tuple) or \
                received[0] != const.MSG_TYPE_HANDSHAKE or \
                len(received) != 2 or \
                len(received[1]) != 3:
            raise errors.BadHandshake()

        self.ident, self.version, self.regs = received[1]
        self.dispatcher.add_peer_regs(self, self.regs)

        # hand off connection management to sender_coro and receiver_coro
        self.established.set()

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
        self.established.wait()
        if self._establish_failed:
            return

        try:
            while not self._closing:
                msg = self.send_queue.get()
                if msg is _END:
                    break
                msg = self.dump(msg)
                self.sock.sendall(msg)
        except socket.error:
            pass

        self.on_connection_closed()

    def receiver_coro(self):
        self.established.wait()
        if self._establish_failed:
            return

        try:
            while not self._closing:
                self.handle_incoming(self.recv_one())
        except socket.error:
            pass
        except errors.MessageCutOff, exc:
            if exc.args[0]:
                raise

        self.on_connection_closed()

    def handle_incoming(self, msg):
        self.dispatcher.incoming(self, msg)

    def establish_coro(self):
        self.init_sock()
        if self.initiator:
            self.connect()
        self.establish()

    def start(self):
        # start up the sender/receiver coros
        # (they'll block until self.established is set)
        scheduler.schedule(self.sender_coro)
        scheduler.schedule(self.receiver_coro)

        self.dispatcher.store_peer(self)

        # do the connect and establish in a coro as well
        scheduler.schedule(self.establish_coro)

    def disconnect(self):
        self._closing = True

        # killing sender_coro is easy as it's waiting for local input
        self.send_queue.put(_END)

        # killing receiver_coro is harder, it's waiting on network activity
        self.sock.close()
        self.sock._readable.set()
        self.sock._readable.clear()

    def on_connection_closed(self):
        if not self._closing:
            # if this came from a geniune socket error then _closing is still
            # False and the writer coro doesn't know about any problem yet
            self._closing = True
            self.send_queue.put(_END)

        self.dispatcher.all_peers.pop(self.addr, None)
        self.dispatcher.drop_peer_regs(self)

_END = object()
