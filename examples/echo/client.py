#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import sys
import traceback

import greenhouse
from junction import Node

HOST = "127.0.0.1"
PORT = 12345

RELAY_ADDR = (HOST, 9100)
SERVICE_ADDR = (HOST, 9000)

SERVICE = 1
RELAY_SERVICE = 2


greenhouse.add_exception_handler(traceback.print_exception)


def main():
    service = RELAY_SERVICE if '-r' in sys.argv else SERVICE
    peer_addr = RELAY_ADDR if '-r' in sys.argv else SERVICE_ADDR

    node = Node((HOST, PORT), [peer_addr])

    node.start()
    if node.wait_on_connections(timeout=3):
        raise RuntimeError("connection timeout")

    print node.rpc(service, "echo", 0, ('one',), {})

    rpcs = map(lambda msg: node.send_rpc(service, "echo", 0, (msg,), {}),
            ('two', 'three', 'four', 'five'))
    while rpcs:
        rpc = node.wait_any_rpc(rpcs)
        rpcs.remove(rpc)
        print rpc.results

if __name__ == '__main__':
    main()
