#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
from junction import Node

HOST = "127.0.0.1"
PORT = 12345

SERVICE_ADDR = (HOST, 9000)

SERVICE = 1


greenhouse.add_exception_handler(traceback.print_exception)


def main():
    node = Node((HOST, PORT), [SERVICE_ADDR])
    node.start()
    node.wait_on_connections()

    print node.rpc(SERVICE, "echo", 0, ('one',), {})

    counters = map(lambda msg: node.send_rpc(SERVICE, "echo", 0, (msg,), {}),
            ('two', 'three', 'four', 'five'))
    print '\n'.join(map(repr, map(node.wait_rpc, counters)))


if __name__ == '__main__':
    main()
