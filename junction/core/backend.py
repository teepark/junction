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
try:
    import eventlet
except ImportError:
    eventlet = None
else:
    import eventlet.greenio
    import eventlet.greenthread
    import eventlet.hubs
    import eventlet.green.Queue


__all__ = ["active", "Socket", "Queue", "Event", "schedule", "schedule_in",
        "schedule_exception", "greenlet", "end", "handle_exception", "pause",
        "getcurrent", "greenlet_class"]

supported = ["greenhouse", "gevent", "eventlet"]
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
    globals()['pause_for'] = greenhouse.pause_for
    globals()['getcurrent'] = greenhouse.getcurrent
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
        gevent.spawn_later(secs, target, *args, **(kwargs or {}))
    return target

def gevent_schedule_exception(exception, target):
    gevent.kill(target, exception)

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
    globals()['schedule_exception'] = gevent_schedule_exception
    globals()['greenlet'] = gevent_greenlet
    globals()['end'] = gevent.kill
    globals()['handle_exception'] = traceback.print_exception
    globals()['pause'] = gevent.sleep
    globals()['pause_for'] = gevent.sleep
    globals()['getcurrent'] = gevent.getcurrent
    globals()['active'] = "gevent"


def eventlet_schedule(target=None, args=(), kwargs=None):
    if target is None:
        def decorator(target):
            return eventlet_schedule(target, args=args, kwargs=kwargs)
        return decorator
    if not isinstance(target, eventlet.greenthread.GreenThread):
        eventlet.spawn(target, *args, **(kwargs or {}))
    return target

def eventlet_schedule_in(secs, target=None, args=(), kwargs=None):
    if target is None:
        def decorator(target):
            return eventlet_schedule_in(secs, target, args=args, kwargs=kwargs)
        return decorator
    if not isinstance(target, eventlet.greenthread.GreenThread):
        eventlet.spawn_after(secs, target, *args, **(kwargs or {}))
    return target

def eventlet_schedule_exception(exception, target):
    eventlet.hubs.get_hub().schedule_call_global(0, target.throw, exception)

def eventlet_greenlet(func, args=(), kwargs=None):
    return eventlet.spawn(func, *args, **(kwargs or {}))

class eventlet_event(object):
    def __init__(self):
        self._waiters = []
        self._is_set = False
        self._activations = {}
        self._activation_count = 0

    def set(self):
        self._is_set = True
        hub = eventlet.hubs.get_hub()
        if self._waiters:
            for waiter in self._waiters:
                hub.schedule_call_global(0, waiter.switch)
            del self._waiters[:]

    def clear(self):
        self._is_set = False

    def wait(self, timeout=None):
        if self._is_set:
            return False

        current = eventlet.getcurrent()
        hub = eventlet.hubs.get_hub()

        if timeout is not None:
            self._activation_count += 1
            counter = self._activation_count
            self._activations[counter] = current
            hub.schedule_call_global(timeout, self._on_timeout, counter)

        self._waiters.append(current)
        hub.switch()

        if timeout is None:
            return False

        return not self._activations.pop(counter, None)

    def _on_timeout(self, counter):
        waiter = self._activations.pop(counter, None)
        if waiter:
            self._waiters.remove(waiter)
            eventlet.hubs.get_hub().schedule_call_global(0, waiter.switch)

def activate_eventlet():
    globals()['Socket'] = eventlet.greenio.GreenSocket
    globals()['Queue'] = eventlet.green.Queue.Queue
    globals()['Event'] = eventlet_event
    globals()['schedule'] = eventlet_schedule
    globals()['schedule_in'] = eventlet_schedule_in
    globals()['schedule_exception'] = eventlet_schedule_exception
    globals()['greenlet'] = eventlet_greenlet
    globals()['end'] = eventlet.kill
    globals()['handle_exception'] = traceback.print_exception
    globals()['pause'] = eventlet.sleep
    globals()['pause_for'] = eventlet.sleep
    globals()['getcurrent'] = eventlet.getcurrent
    globals()['active'] = "eventlet"


def activate():
    if greenhouse:
        activate_greenhouse()
    elif gevent:
        activate_gevent()
    elif eventlet:
        activate_eventlet()
    else:
        raise RuntimeError("no supported greenlet runtime found out of (%s)" %
                ", ".join(supported))

activate()
