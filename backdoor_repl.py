#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import errno
import logging
import os
import sys
import traceback

import greenhouse
import junction

subprocess = greenhouse.patched("subprocess")


# turn on greenhouse and junction logging, and exception printing
greenhouse.configure_logging(level=1)
junction.configure_logging(level=1)
greenhouse.global_exception_handler(traceback.print_exception)

# except quiet the greenhouse scheduler's logger since the backdoor
# sets and unsets hooks with every REPL line
logging.getLogger("greenhouse.scheduler").setLevel(logging.WARNING)


BACKDOOR_PORT = 9123

def run_backdoor(finished):
    global BACKDOOR_PORT

    try:
        while 1:
            try:
                greenhouse.run_backdoor(("127.0.0.1", BACKDOOR_PORT),
                        {'greenhouse': greenhouse, 'junction': junction})
                break
            except EnvironmentError, exc:
                if exc.args[0] != errno.EADDRINUSE:
                    raise
                BACKDOOR_PORT += 1
    finally:
        finished.set()


def main(environ, argv):
    finished = greenhouse.Event()
    backdoor = greenhouse.greenlet(run_backdoor, args=(finished,))
    greenhouse.schedule(backdoor)

    # nothing in run_backdoor blocks until it succeeds in
    # getting a port, so this can just be a simple pause
    greenhouse.pause()

    telnet = subprocess.Popen(['telnet', '127.0.0.1', str(BACKDOOR_PORT)])
    rc = telnet.wait()

    greenhouse.end(backdoor)
    finished.wait()
    return rc


if __name__ == '__main__':
    exit(main(os.environ, sys.argv))
