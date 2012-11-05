import traceback

try:
    import greenhouse
except ImportError:
    greenhouse = None
try:
    import gevent
except ImportError:
    gevent = None
else:
    import gevent.event
    import gevent.queue
    import gevent.socket


__all__ = ["active", "Socket", "Queue", "Event", "schedule", "schedule_in",
        "schedule_exception", "greenlet", "end", "handle_exception", "pause",
        "getcurrent", "greenlet_class"]

supported = ["greenhouse", "gevent"]
active = None


def activate_greenhouse():
    globals()['Socket'] = greenhouse.Socket
    globals()['Queue'] = greenhouse.Queue
    globals()['Event'] = greenhouse.Event
    globals()['schedule'] = greenhouse.schedule
    globals()['schedule_in'] = greenhouse.schedule_in
    globals()['schedule_exception'] = greenhouse.schedule_exception
    globals()['greenlet'] = greenhouse.greenlet
    globals()['end'] = greenhouse.end
    globals()['handle_exception'] = greenhouse.handle_exception
    globals()['pause'] = greenhouse.pause
    globals()['getcurrent'] = greenhouse.getcurrent()
    globals()['active'] = "greenhouse"


def gevent_schedule(target=None, args=(), kwargs=None):
    if target is None:
        def decorator(target):
            return gevent_schedule(target, args=args, kwargs=kwargs)
        return decorator
    if isinstance(target, gevent.Greenlet):
        target.start()
    else:
        gevent.spawn(target, *args, **(kwargs or {}))
    return target

def gevent_schedule_in(secs, target=None, args=(), kwargs=None):
    if target is None:
        def decorator(target):
            return gevent_schedule_in(secs, target, args=args, kwargs=kwargs)
        return decorator
    if isinstance(target, gevent.Greenlet):
        target.start_later(secs)
    else:
        gevent.spawn_later(secs, target, *args, **(kwargs or None))
    return target

def gevent_greenlet(func, args=(), kwargs=None):
    return gevent.Greenlet(func, *args, **(kwargs or {}))

class gevent_event(gevent.event.Event):
    def wait(self, *args, **kwargs):
        return not super(gevent_event, self).wait(*args, **kwargs)

def activate_gevent():
    globals()['Socket'] = gevent.socket.socket
    globals()['Queue'] = gevent.queue.Queue
    globals()['Event'] = gevent_event
    globals()['schedule'] = gevent_schedule
    globals()['schedule_in'] = gevent_schedule_in
    globals()['greenlet'] = gevent_greenlet
    globals()['end'] = gevent.kill
    globals()['handle_exception'] = traceback.print_exception
    globals()['pause'] = gevent.sleep
    globals()['getcurrent'] = gevent.getcurrent
    globals()['active'] = "gevent"


def activate_best():
    if greenhouse:
        activate_greenhouse()
    elif gevent:
        activate_gevent()
    else:
        raise RuntimeError("no supported greenlet runtime " +
                "(%s) found" % ", ".join(supported))

activate_best()
