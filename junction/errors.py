class MessageCutOff(Exception):
    pass

class BadHandshake(Exception):
    pass

class Unroutable(Exception):
    pass

class NoRemoteHandler(Exception):
    pass

class RemoteException(Exception):
    pass

class UnrecognizedRemoteProblem(Exception):
    pass

class RPCWaitTimeout(Exception):
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
