#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import random
import traceback

import greenhouse
import junction

greenhouse.global_exception_handler(traceback.print_exception)


SERVICE_PORT = 9870
WAIT_SERVICE = 1


def main():
    client = junction.Client(("localhost", SERVICE_PORT))
    client.connect()
    client.wait_connected()

    print "wait 2"
    client.rpc(WAIT_SERVICE, 0, "wait", (2,))

    rpcs = []
    for i in xrange(5):
        wait = random.random() * 5
        rpc = client.send_rpc(WAIT_SERVICE, 0, "wait", (wait,))
        rpc.counter = i
        rpcs.append(rpc)
        print "queued a wait %r: %r" % (rpcs[-1].counter, wait)

    while rpcs:
        rpc = junction.wait_any(rpcs)
        print "got back %r" % rpc.counter
        rpcs.remove(rpc)


if __name__ == '__main__':
    main()
