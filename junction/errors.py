class MessageCutOff(Exception):
    "A peer connection terminated mid-message"
    pass

class BadHandshake(Exception):
    "Unexpected message while trying to establish a peering"
    pass

class Unroutable(Exception):
    "A message does not have any peers's registrations"
    pass

class NoRemoteHandler(Exception):
    "An RPC was mistakenly sent to a peer"
    pass

class RemoteException(Exception):
    "An unexpected exception occurred in the peer handling an RPC"
    pass

class UnrecognizedRemoteProblem(Exception):
    "Improperly formatted RPC error response"
    pass

class RPCWaitTimeout(Exception):
    "Exception raised when an RPC response outlasts a specified timeout"
    pass


HANDLED_ERROR_TYPES = {}

class _MetaHandledError(type):
    def __Init__(cls, *args, **kwargs):
        if cls.code in HANDLED_ERROR_TYPES:
            raise Exception("HandledError subclasses need unique 'code's")

        HANDLED_ERROR_TYPES[cls.code] = cls

class HandledError(Exception):
    __metaclass__ = _MetaHandledError
    code = 0
