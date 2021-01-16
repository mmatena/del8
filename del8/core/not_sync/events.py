"""Attempt at a framework for handling multithreading/multiprocessing."""
import abc
import collections
from concurrent import futures
import functools
import inspect
import queue
import uuid as uuidlib

from absl import logging
from overload import overload

from del8.core.utils import decorator_util as dec_util


_EventKey = collections.namedtuple("_EventKey", ["namespace", "name"])
_ExceptionHandler = collections.namedtuple(
    "_ExceptionHandler", ["exception", "handler"]
)


class Event(object):
    def __init__(self, name, data, namespace=None):
        self.namespace = namespace
        self.name = name
        self.data = data

    def key(self):
        return _EventKey(namespace=self.namespace, name=self.name)


_CONTEXT_STACK = []


class Context(object):
    def __init__(self, pool):
        self._pool = pool
        self._events_queue = queue.Queue()
        self._handlers = collections.defaultdict(list)
        self._event_listeners = collections.defaultdict(list)
        self._exception_handlers = collections.defaultdict(list)

        self._pending_futures = set()
        self._pending_future_to_event = {}

    ############################################

    @overload
    def add_event(self, namespace: str, name: str, data):
        event = Event(namespace=namespace, name=name, data=data)
        self._events_queue.put_nowait(event)

    @add_event.add
    def add_event(self, name: str, data):
        self.add_event(None, name, data)

    ############################################

    @overload
    def add_handler(self, namespace: str, name: str, handler):
        key = _EventKey(namespace=namespace, name=name)
        self._handlers[key].append(handler)

    @add_handler.add
    def add_handler(self, name: str, handler):
        self.add_handler(None, name, handler)

    ############################################

    def add_event_listener(self, name: str, handler):
        self._event_listeners[name].append(handler)

    ############################################

    @overload
    def add_exception_handler(self, namespace: str, name: str, exception, handler):
        # NOTE: `exception` is a class extending Exception.
        key = _EventKey(namespace=namespace, name=name)
        exception_handler = _ExceptionHandler(exception=exception, handler=handler)
        self._exception_handlers[key].append(exception_handler)

    @add_exception_handler.add
    def add_exception_handler(self, namespace: str, name: str, handler):
        self.add_exception_handler(namespace, name, Exception, handler)

    @add_exception_handler.add
    def add_exception_handler(self, name: str, exception, handler):
        self.add_exception_handler(None, name, exception, handler)

    @add_exception_handler.add
    def add_exception_handler(self, name: str, handler):
        self.add_exception_handler(None, name, Exception, handler)

    ############################################

    def _get_event_handlers(self, event: Event):
        key = event.key()
        return self._handlers[key] + self._event_listeners[key.name]

    def _get_event_exception_handlers(self, event: Event):
        # Returns sorted in descending order of inheritence depth.
        exception_handlers = self._exception_handlers[event.key()]
        return sorted(
            exception_handlers, key=lambda h: len(h.exception.mro()), reverse=True
        )

    def _handle_event(self, event: Event):
        logging.info(f"Handling event {event.name} in namespace {event.namespace}")
        for fn in self._get_event_handlers(event):
            arg = event if fn._pass_full_event else event.data
            future = self._pool.submit(fn, arg)
            self._pending_future_to_event[future] = event
            self._pending_futures.add(future)

    def _handle_event_exception(self, event: Event, e: Exception):
        caught = False
        for e_cls, fn in self._get_event_exception_handlers(event):
            if not isinstance(e, e_cls):
                continue
            arg = event if fn._pass_full_event else event.data
            future = self._pool.submit(fn, arg, e)
            self._pending_future_to_event[future] = event
            self._pending_futures.add(future)
            caught = True
        if not caught:
            raise e

    ############################################

    def _execute_all_in_queue(self):
        while True:
            try:
                self._handle_event(self._events_queue.get_nowait())
            except queue.Empty:
                break

    def _process_completed_futures(self, completed):
        for future in completed:
            event = self._pending_future_to_event[future]
            del self._pending_future_to_event[future]
            # TODO: Find some nice, thread-safe way to do this step in another
            # thread. I've had strange issues in the past where an exception occurs
            # during the execution of the future and then the call to result blocks
            # for some reason and freezes the main thread. Note that typically does
            # not happen during exceptions and I have no idea what is causing that.
            e = future.exception()
            if e is not None:
                self._handle_event_exception(event, e)

    def execute(self):
        # Creates futures for all of the events already in the queue. Blocks until
        # there are no more pending futures.
        self._execute_all_in_queue()
        while self._pending_futures or not self._events_queue.empty():
            # self._execute_all_in_queue()
            completed, self._pending_futures = futures.wait(
                self._pending_futures, return_when=futures.FIRST_COMPLETED
            )
            self._process_completed_futures(completed)
            self._execute_all_in_queue()

    ############################################

    def __enter__(self):
        assert not _CONTEXT_STACK, "TODO: Figure out what nested contexts should do."
        _CONTEXT_STACK.append(self)
        return self

    def __exit__(self, type, value, traceback):
        me = _CONTEXT_STACK.pop()
        assert self is me


class MultiThreadedContext(Context):
    def __init__(self, max_workers):
        pool = futures.ThreadPoolExecutor(max_workers=max_workers)
        super().__init__(pool)


###############################################################################


def _new_uuid():
    return uuidlib.uuid4().hex


class EventItemWithUuid(abc.ABC):
    # IMPORTANT NOTE: Any state that is set and modified on instances should be threadsafe.
    #
    # TODO: Add some utilities for this, like maybe provide something like a state dict that
    # can only be interacted with in thread-safe ways. I could also provide a decorator that
    # locks the object during each method call, but there might be drawbacks to that.

    def __init__(self, uuid=None):
        if uuid is None:
            uuid = _new_uuid()
        self._uuid = uuid

        assert (
            _CONTEXT_STACK
        ), "No context on the stack. Please use something like `with ctx:`."
        self._context = _CONTEXT_STACK[-1]

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        for _, method in methods:
            for name in getattr(method, "_event_handler_items", []):
                self._context.add_handler(self.uuid, name, method)
            for name in getattr(method, "_event_listener_items", []):
                self._context.add_event_listener(name, method)
            for name, exception in getattr(
                method, "_event_exception_handler_items", []
            ):
                self._context.add_exception_handler(self.uuid, name, exception, method)

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def context(self) -> Event:
        return self._context


def _default_attr(ctr, obj, attr):
    if not hasattr(obj, attr):
        setattr(obj, attr, ctr())
    return getattr(obj, attr)


def event_generator(name):
    def dec(method):
        @functools.wraps(method)
        def fn(self, *args, **kwargs):
            assert isinstance(self, EventItemWithUuid)
            data = method(self, *args, **kwargs)
            self.context.add_event(self.uuid, name, data)
            return data

        return fn

    return dec


def event_handler(name, *, pass_full_event=False):
    def dec(method):
        _default_attr(set, method, "_event_handler_items").add(name)
        if getattr(method, "_pass_full_event", pass_full_event) != pass_full_event:
            # TODO: In the unlikely case that this is an issue, it should be relatively
            # easy to make this possible.
            raise TypeError(
                f"Event handler {method.__name__} must either always pass a "
                "full event or pass just the event data for all event types."
            )
        method._pass_full_event = pass_full_event
        return method

    return dec


def event_listener(name, *, pass_full_event=False):
    def dec(method):
        _default_attr(set, method, "_event_listener_items").add(name)
        if getattr(method, "_pass_full_event", pass_full_event) != pass_full_event:
            # TODO: In the unlikely case that this is an issue, it should be relatively
            # easy to make this possible.
            raise TypeError(
                f"Event listener {method.__name__} must either always pass a "
                "full event or pass just the event data for all event types."
            )
        method._pass_full_event = pass_full_event
        return method

    return dec


def event_exception_handler(name, exception, *, pass_full_event=False):
    def dec(method):
        _default_attr(set, method, "_event_exception_handler_items").add(
            (name, exception)
        )
        if getattr(method, "_pass_full_event", pass_full_event) != pass_full_event:
            # TODO: In the unlikely case that this is an issue, it should be relatively
            # easy to make this possible.
            raise TypeError(
                f"Event handler {method.__name__} must either always pass a "
                "full event or pass just the event data for all event types."
            )
        method._pass_full_event = pass_full_event
        return method

    return dec


###############################################################################


class DfsaWithUuid(EventItemWithUuid):
    # TODO: Create transitions for handling of errors within states.

    # A collection of unique strings.
    STATES = frozenset()
    INITIAL_STATE = None
    TERMINAL_STATE = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._finite_state = None
        self._change_state(self.INITIAL_STATE)

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        remaining_src_states = set(self.STATES) - {self.TERMINAL_STATE}
        for _, method in methods:

            if hasattr(method, "_src_state"):
                src_state = method._src_state
                assert src_state != self.TERMINAL_STATE
                assert src_state in self.STATES
                self._add_dfsa_transition(method, src_state)
                del remaining_src_states[src_state]

            elif hasattr(method, "_fixed_transition"):
                src_state, dst_state = method._fixed_transition
                assert src_state != self.TERMINAL_STATE
                assert src_state in self.STATES
                assert dst_state != self.INITIAL_STATE
                assert dst_state in self.STATES
                self._add_fixed_transition(method, src_state, dst_state)
                del remaining_src_states[src_state]

        if remaining_src_states:
            # The minus one is due to the terminal state.
            raise TypeError(
                f"The DFSA states {remaining_src_states} were missing a handler."
            )

    @property
    def finite_state(self):
        return self._finite_state

    def _change_state(self, dst_state):
        assert dst_state != self.INITIAL_STATE
        assert dst_state in self.STATES
        self._finite_state = dst_state
        if dst_state != self.TERMINAL_STATE:
            self.context.add_event(self.uuid, dst_state, None)

    def _add_dfsa_transition(self, method, src_state):
        # TODO: Maybe check somewhere that this is the only transition from src_state.
        def handler(data):
            dst_state = method()
            self._change_state(dst_state)

        self._context.add_handler(self.uuid, src_state, handler)

    def _add_fixed_transition(self, method, src_state, dst_state):
        # TODO: Maybe check somewhere that this is the only transition from src_state.
        def handler(data):
            method()
            self._change_state(dst_state)

        self._context.add_handler(self.uuid, src_state, handler)


def dfsa_transition(src_state):
    def dec(method):
        method._src_state = src_state
        return method

    return dec


def fixed_transition(src_state, dst_state):
    def dec(method):
        method._fixed_transition = (src_state, dst_state)
        return method

    return dec
