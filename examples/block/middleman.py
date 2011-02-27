#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
import junction

greenhouse.add_exception_handler(traceback.print_exception)


PORT = 9870
SERVICE_PORT = 9876


def main():
    node = junction.Node(("localhost", PORT), [("localhost", SERVICE_PORT)])
    node.start()

    greenhouse.schedule(greenhouse.run_backdoor,
            args=(("localhost", PORT + 1), {'node': node}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
