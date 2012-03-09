#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import functools
import sys
import traceback

import greenhouse
import junction

HOST = "127.0.0.1"
PORT = 12345

RELAY_ADDR = (HOST, 9100)
SERVICE_ADDR = (HOST, 9000)

SERVICE = 1


greenhouse.global_exception_handler(traceback.print_exception)


def second_call(client, results):
    return client.send_rpc(SERVICE, "echo", 0, (results[0],), {})

def third_call(results):
    return results[0][:-1]


def main():
    peer_addr = RELAY_ADDR if '-r' in sys.argv else SERVICE_ADDR

    client = junction.Client(peer_addr)

    client.connect()
    if client.wait_on_connections(timeout=3):
        raise RuntimeError("connection timeout")

    print client.rpc(SERVICE, "echo", 0, ('one',), {})

    rpcs = map(lambda msg: client.send_rpc(SERVICE, "echo", 0, (msg,), {}),
            ('two', 'three', 'four', 'five'))

    dependents = [
            rpc.after(functools.partial(second_call, client)).after(third_call)
            for rpc in rpcs]

    while dependents:
        dep = client.wait_any(dependents)
        dependents.remove(dep)
        print dep.results

if __name__ == '__main__':
    main()
