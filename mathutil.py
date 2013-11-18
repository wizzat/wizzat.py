import math

__all__ = [
    'Percentile',
]

def avg(values):
    return 1.0 * sum(values) / len(values)

class Avg(object):
    pass

class Percentile(object):
    def __init__(self, *values):
        self.values     = [ 0 ] * 2000
        self.total      = 0
        self.num_values = 0
        self.min_idx    = None
        self.max_idx    = None

        for value in values:
            self.add_value(value)

    def add_value(self, value):
        if value == None:
            return

        idx = int(math.log10(value+1) * 100)

        self.total += value
        self.values[idx] += 1
        self.num_values += 1
        self.min_idx = min(x for x in (idx, self.min_idx) if x != None)
        self.max_idx = max(x for x in (idx, self.max_idx) if x != None)

    def percentile(self, pct):
        if pct > 1.0:
            raise ValueError("pct > 1.0")

        if self.min_idx == None:
            return None

        target = pct * self.num_values
        ct = 0
        idx = self.min_idx

        for idx in xrange(self.min_idx, self.max_idx+1):
            ct += self.values[idx]
            if ct >= target:
                break

        def e(n):
            return 10**(n/100.0)-.5

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
