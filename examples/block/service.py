#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4


import greenhouse
import junction


PORT = 9876
MIDDLEMAN_PORT = 9870

WAIT_SERVICE = 1


def wait(seconds):
    greenhouse.pause_for(seconds)


def main():
    node = junction.Node(("localhost", PORT), [("localhost", MIDDLEMAN_PORT)])
    node.start()

    node.accept_rpc(WAIT_SERVICE, "wait", 0, 0, wait)

    greenhouse.schedule(greenhouse.run_backdoor,
            args=(("localhost", PORT + 1), {'node': node}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
