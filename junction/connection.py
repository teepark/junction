from __future__ import absolute_import

import socket
import struct

from greenhouse import scheduler, utils
import mummy

from . import const, errors


class Peer(object):
    def __init__(self, dispatcher, addr, sock, connected=False):
        self.dispatcher = dispatcher
        self.addr = addr
        self.sock = sock

        self._sock_inited = False
        self._closing = False
        self.connected = utils.Event()
        if connected:
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
        self.init_sock()
        self.sock.connect(self.addr)
        self.connected.set()

    def establish(self):
        self.connected.wait()

        # send a message providing information about ourself
        try:
            self.sock.sendall(self.dump((const.MSG_TYPE_HANDSHAKE, (
                    self.dispatcher.addr,
                    self.dispatcher.version,
                    list(self.dispatcher.local_registrations())))))
        except socket.error:
            return

        # expect to get a similar message back from the peer
        try:
            received = self.recv_one()
        except socket.error, exc:
            return
        except MessageCutOff, exc:
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
        self.connected.wait()
        self.established.wait()

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
        self.connected.wait()
        self.established.wait()

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

    def establish_coro(self, connect):
        if connect:
            self.connect()
        self.establish()

    def start(self, connect=True):
        # start up the sender/receiver coros
        # (they'll block until self.established is set)
        scheduler.schedule(self.sender_coro)
        scheduler.schedule(self.receiver_coro)

        self.dispatcher.store_peer(self)

        # do the connect and establish in a coro as well
        scheduler.schedule(self.establish_coro, args=(connect,))

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
