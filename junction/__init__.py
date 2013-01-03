from __future__ import absolute_import

import logging
import sys

from .hub import Hub
from .client import Client
from .core.backend import activate_greenhouse, activate_gevent, activate_eventlet


VERSION = (1, 0, 1, "")
__version__ = ".".join(filter(None, map(str, VERSION)))


def configure_logging(filename=None, filemode=None, fmt=None,
        level=logging.INFO, stream=None, handler=None):
    if handler is None:
        if filename is None:
            handler = logging.StreamHandler(stream or sys.stderr)
        else:
            handler = logging.FileHandler(filename, filemode or 'a')

    if fmt is None:
        fmt = "[%(asctime)s] %(name)s/%(levelname)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))

    log = logging.getLogger("junction")
    log.setLevel(level)
    log.addHandler(handler)
