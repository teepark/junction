#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import random

import junction


SERVICE_PORT = 9876
WAIT_SERVICE = 1


def main():
    node = junction.Node(("localhost", 12345), [("localhost", SERVICE_PORT)])
    node.start()
    node.wait_on_connections()

    print "wait 2"
    node.rpc(WAIT_SERVICE, "wait", 0, (2,), {})

    rpcs = []
    for i in xrange(5):
        wait = random.random() * 5
        rpcs.append(node.send_rpc(WAIT_SERVICE, "wait", 0, (wait,), {}))
        print "queued a wait %r: %r" % (rpcs[-1].counter, wait)

    while rpcs:
        rpc = node.wait_any_rpc(rpcs)
        print "got back %r" % rpc.counter
        rpcs.remove(rpc)


if __name__ == '__main__':
    main()
