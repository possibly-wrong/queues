"""Publish/subscribe discrete event simulation framework."""

import heapq
from collections import namedtuple, defaultdict

__all__ = ['Event', 'Simulation']

Event = namedtuple('Event', ('time', 'priority', 'type', 'data'))
Event.__new__.__defaults__ = (float('-inf'), 0, None, None)

class Simulation:
    """Manage events and listeners for a single simulation."""

    def __init__(self):
        """Create an "empty" simulation with no events or listeners."""
        self._queue = []
        self._listeners = defaultdict(list)
        self._end_time = float('inf')

    def publish(self, event):
        """Insert Event instance into the queue.

        Events are sorted first by event.time, then by event.priority (lower
        values first).
        """
        heapq.heappush(self._queue, event)

    def subscribe(self, event_type, listener):
        """Register listener callback for events of the given type.

        listener(self, event) will be called for each published event of the
        given immutable event.type, where self is this simulation instance.
        """
        self._listeners[event_type].append(listener)

    def stop_at(self, time):
        """Set simulation end time."""
        self._end_time = time

    def run(self):
        """Run simulation, returning the simulation end time."""
        now = float('-inf')
        while self._queue:
            event = heapq.heappop(self._queue)
            assert(event.time >= now)
            now = event.time
            if now >= self._end_time:
                now = self._end_time
                break
            for listener in self._listeners[event.type]:
                listener(self, event)
        return now
