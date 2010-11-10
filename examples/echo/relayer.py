#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
from junction import Node

HOST = "127.0.0.1"
PORT = 9100

SERVICE_HOST = HOST
SERVICE_PORT = 9000

BDHOST = HOST
BDPORT = 9101

FRONT_SERVICE = 2
BACK_SERVICE = 1

greenhouse.add_exception_handler(traceback.print_exception)

node = Node((HOST, PORT), [(SERVICE_HOST, SERVICE_PORT)])


def handler(x):
    return "forwarded %s" % node.rpc(BACK_SERVICE, "echo", 0, (x,), {})[0]


def main():
    node.accept_rpc(FRONT_SERVICE, "echo", 0, 0, handler)

    node.start()
    node.wait_on_connections()

    greenhouse.schedule(greenhouse.run_backdoor,
            args=((BDHOST, BDPORT), {'node': node}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
