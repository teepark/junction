#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
import junction


PORT = 9876
MIDDLEMAN_PORT = 9870

WAIT_SERVICE = 1


greenhouse.global_exception_handler(traceback.print_exception)
junction.configure_logging(level=1)

def wait(seconds):
    greenhouse.pause_for(seconds)


def main():
    hub = junction.Hub(("localhost", PORT), [("localhost", MIDDLEMAN_PORT)])
    hub.start()

    hub.accept_rpc(WAIT_SERVICE, 0, 0, "wait", wait)

    greenhouse.schedule(greenhouse.run_backdoor,
            args=(("localhost", PORT + 1), {'hub': hub}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
