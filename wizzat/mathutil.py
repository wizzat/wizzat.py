from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *
from future.utils import iteritems

import collections
import itertools
import math

from wizzat.dateutil import now, to_epoch

__all__ = [
    'avg',
    'Percentile',
    'RollingTimePercentile',
]

def avg(iterable):
    """
    Returns the average (mean) of all values in the iterable.
    """
    return 1.0 * sum(iterable) / len(iterable)


class RollingTimePercentile(object):
    """
    Logarithmic percentile approximation for non-negative values.
    Expected variance from actual median is about 5%.
    Expected memory consumption is ~8kb with constant time calculation.

    p = Percentile()
    for x in iterable:
        p.add_value(x)

    p.percentile(0.0) # Min value
    p.percentile(0.5) # Median
    p.percentile(1.0) # Max value
    """
    def __init__(self, window_sec, max_sec):
        self.window_sec = window_sec
        self.max_sec = max_sec
        self.windows = {}

    def add_value(self, value):
        curr_window = to_epoch(now()) // self.window_sec
        if curr_window not in self.windows:
            self.windows[curr_window] = Percentile()
            self.trim_windows()

        self.windows[curr_window].add_value(value)

    def trim_windows(self):
        curr_window = to_epoch(now()) // self.window_sec
        min_window = (to_epoch(now()) - self.max_sec) // self.window_sec

        for key in list(self.windows.keys()):
            if key < min_window:
                self.windows.pop(key)

    def percentile(self, pct):
        if not 0.0 <= pct <= 1.0:
            raise ValueError()

        self.trim_windows()

        if not self.windows:
            return None

        if pct == 0.0:
            return min(x.min_value for x in self.windows.values())
        elif pct == 1.0:
            return max(x.max_value for x in self.windows.values())

        num_values = 0
        all_values = []
        for window in self.windows.values():
            num_values += window.num_values
            all_values.extend(window.values.items())
        all_values.sort(key=lambda x: x[0])

        ct = 0
        target = pct * num_values
        for idx, idx_ct in all_values:
            ct += idx_ct
            if ct >= target:
                break

        def e(n):
            return 10**(n/100.0)

        return int(avg([e(idx), e(idx+.9)]))

    def _window_value(self, op, default):
        self.trim_windows()
        if not self.windows:
            return None

        value = default
        for window in self.windows.values():
            value = op(value, window)

        return value

    @property
    def num_values(self):
        return self._window_value(lambda v, w: v + w.num_values, 0)

    @property
    def total(self):
        return self._window_value(lambda v, w: v + w.total, 0)

    @property
    def min_value(self):
        return self._window_value(lambda v, w: min(v, w.min_value), float("inf"))

    @property
    def max_value(self):
        return self._window_value(lambda v, w: max(v, w.min_value), -1)


class Percentile(object):
    """
    Logarithmic percentile approximation for non-negative values.
    Expected variance from actual median is about 5%.
    Expected memory consumption is ~8kb with constant time calculation.

    p = Percentile()
    for x in iterable:
        p.add_value(x)

    p.percentile(0.0) # Min value
    p.percentile(0.5) # Median
    p.percentile(1.0) # Max value
    """
    def __init__(self, *values):
        self.values     = collections.defaultdict(int)
        self.total      = 0
        self.num_values = 0
        self.max_value  = -1
        self.min_value  = float("inf")

        for value in values:
            self.add_value(value)

    def add_value(self, value):
        if value is None:
            return

        if value < 0:
            raise ValueError(value)

        if value < self.min_value:
            self.min_value = value

        if value > self.max_value:
            self.max_value = value

        self.total += value
        self.num_values += 1

        if value == 0:
            value = 0.0000001

        idx = int(math.log10(value) * 100)
        self.values[idx] += 1

    def percentile(self, pct):
        if not 0.0 <= pct <= 1.0:
            raise ValueError()

        if not self.values:
            return None

        if pct == 0.0:
            return self.min_value
        elif pct == 1.0:
            return self.max_value

        target = pct * self.num_values
        ct = 0

        for idx, value in sorted(iteritems(self.values)):
            ct += value
            if ct >= target:
                break

        def e(n):
            return 10**(n/100.0)

        return int(avg([e(idx), e(idx+.9)]))



    def __repr__(self):
        return "Percentile<min={},25={},50={},75={},98={},max={}>".format(
            self.percentile(0.0),
            self.percentile(.25),
            self.percentile(.50),
            self.percentile(.75),
            self.percentile(.98),
            self.percentile(1.0),
        )
