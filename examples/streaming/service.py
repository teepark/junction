#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import os
import sys
import traceback

import greenhouse
import junction


def handle(chunks):
    for chunk in chunks:
        yield "echoed: %s" % chunk

def main(environ, argv):
    greenhouse.global_exception_handler(traceback.print_exception)
    junction.configure_logging(level=1)

    hub = junction.Hub(("", 9056), [])
    hub.accept_rpc('service', 0, 0, 'echostream', handle)
    hub.start()

    greenhouse.Event().wait()


if __name__ == '__main__':
    exit(main(os.environ, sys.argv))
