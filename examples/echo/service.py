#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
from junction import Node

HOST = "127.0.0.1"
PORT = 9000

RELAY_HOST = HOST
RELAY_PORT = 9100

BACKHOST = HOST
BACKPORT = 9001

SERVICE = 1


greenhouse.add_exception_handler(traceback.print_exception)

def handler(x):
    return "echo: %s" % x


def main():
    node = Node((HOST, PORT), [(RELAY_HOST, RELAY_PORT)])
    node.start()

    node.accept_rpc(SERVICE, "echo", 0, 0, handler)

    greenhouse.schedule(greenhouse.run_backdoor,
            args=((BACKHOST, BACKPORT), {'node': node}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
