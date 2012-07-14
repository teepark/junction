#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import traceback

import greenhouse
import junction

greenhouse.global_exception_handler(traceback.print_exception)
junction.configure_logging(level=1)


PORT = 9870
SERVICE_PORT = 9876


def main():
    hub = junction.Hub(("localhost", PORT), [("localhost", SERVICE_PORT)])
    hub.start()

    greenhouse.schedule(greenhouse.run_backdoor,
            args=(("localhost", PORT + 1), {'hub': hub}))

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
