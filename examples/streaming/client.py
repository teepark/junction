#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import os
import sys
import traceback

import greenhouse
import junction


def gen_input():
    while 1:
        line = greenhouse.stdin.readline()
        if line == '\n':
            break
        yield line[:-1]


def main(environ, argv):
    greenhouse.global_exception_handler(traceback.print_exception)
    junction.configure_logging(level=1)

    port = 9056
    if argv[1:] and argv[1] == 'relay':
        port += 1

    client = junction.Client(("", port))
    client.connect()
    client.wait_on_connections()

    for line in client.rpc(
            'service', 0, 'echostream', (gen_input(),), singular=1):
        print 'line:', line


if __name__ == '__main__':
    exit(main(os.environ, sys.argv))
