#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
from junction import Hub

HOST = "127.0.0.1"
PORT = 9100

SERVICE_HOST = HOST
SERVICE_PORT = 9000

BDHOST = HOST
BDPORT = 9101

SERVICE = 1

greenhouse.global_exception_handler(traceback.print_exception)

hub = Hub((HOST, PORT), [(SERVICE_HOST, SERVICE_PORT)])


def main():
    hub.start()
    hub.wait_on_connections()

    greenhouse.schedule(greenhouse.run_backdoor,
            args=((BDHOST, BDPORT), {'hub': hub}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
