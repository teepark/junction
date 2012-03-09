from __future__ import absolute_import

from .node import Node
from .client import Client


VERSION = (0, 1, 0, "")
__version__ = ".".join(filter(None, map(str, VERSION)))

Node.VERSION = Client.VERSION = VERSION
