#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import random
import traceback

import greenhouse
import junction

greenhouse.add_exception_handler(traceback.print_exception)


SERVICE_PORT = 9870
WAIT_SERVICE = 1


def main():
    client = junction.Client(("localhost", SERVICE_PORT))
    client.connect()
    client.wait_on_connections()

    print "wait 2"
    client.rpc(WAIT_SERVICE, "wait", 0, (2,), {})

    rpcs = []
    for i in xrange(5):
        wait = random.random() * 5
        rpcs.append(client.send_rpc(WAIT_SERVICE, "wait", 0, (wait,), {}))
        print "queued a wait %r: %r" % (rpcs[-1].counter, wait)

    while rpcs:
        rpc = client.wait_any_rpc(rpcs)
        print "got back %r" % rpc.counter
        rpcs.remove(rpc)


if __name__ == '__main__':
    main()
