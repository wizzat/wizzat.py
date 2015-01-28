from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import math
import wizzat.testutil
from wizzat.mathutil import Percentile, RollingTimePercentile
from wizzat.dateutil import *

class PercentileTest(wizzat.testutil.TestCase):
    def percentile(self, values):
        p = Percentile()

        for value in values:
            p.add_value(value)

        return p

    def test_percentile__100(self):
        p = self.percentile(range(100))
        self.assertEqual(p.percentile(0.0), 0)
        self.assertEqual(p.percentile(.25), 24)
        self.assertEqual(p.percentile(0.5), 49)
        self.assertEqual(p.percentile(.75), 73)
        self.assertEqual(p.percentile(1.0), 99)

    def test_percentile__1000(self):
        p = self.percentile(range(1000))
        self.assertEqual(p.percentile(0.0), 0)
        self.assertEqual(p.percentile(.25), 248)
        self.assertEqual(p.percentile(0.5), 494)
        self.assertEqual(p.percentile(.75), 749)
        self.assertEqual(p.percentile(1.0), 999)

    def test_percentile_deviation(self):
        r = range(0, 2**64, int(2**64/1000000))
        p = self.percentile(float(x) for x in r)

        for x in range(100):
            real_pct = r[int((x/100.0) * len(r))]
            pct = p.percentile(x/100.0)

            if real_pct != pct:
                variance = 1.0-math.fabs(1.0 * real_pct / pct)
                self.assertTrue(variance < 0.055, "{} != {}, {} {}".format(pct, real_pct, variance, x))

    def test_handles_zero(self):
        p = self.percentile([0, 0, 0, 1])

        self.assertEqual(p.percentile(0.0), 0.0)
        self.assertEqual(p.percentile(.98), 1.0)
        self.assertEqual(p.percentile(1.0), 1.0)

    def test_empty(self):
        p = self.percentile([])
        self.assertEqual(p.percentile(0),   None)
        self.assertEqual(p.percentile(.25), None)
        self.assertEqual(p.percentile(.50), None)
        self.assertEqual(p.percentile(.75), None)
        self.assertEqual(p.percentile(.98), None)
        self.assertEqual(p.percentile(1.0), None)


class RollingPercentileTest(PercentileTest):
    def percentile(self, values):
        p = RollingTimePercentile(10, 60)

        for value in values:
            p.add_value(value)

        return p

    def test_window_rolloff__empty(self):
        set_now("2014-04-04 00:00:00")
        p = self.percentile([])
        p.add_value(1.0)
        set_now("2014-04-04 01:00:01")

        self.assertEqual(p.percentile(0),   None)
        self.assertEqual(p.percentile(.25), None)
        self.assertEqual(p.percentile(.50), None)
        self.assertEqual(p.percentile(.75), None)
        self.assertEqual(p.percentile(.98), None)
        self.assertEqual(p.percentile(1.0), None)

        self.assertEqual(p.max_value, None)
        self.assertEqual(p.min_value, None)
        self.assertEqual(p.num_values, None)
        self.assertEqual(p.total, None)

    def test_window_rolloff(self):
        set_now("2014-04-04 00:00:00")
        p = self.percentile([])
        static_now = now()

        for x in range(100):
            p.add_value(x)
            set_now(static_now + seconds(10*x))

        self.assertEqual(p.percentile(0.0), 94)
        self.assertEqual(p.percentile(.25), 94)
        self.assertEqual(p.percentile(0.5), 96)
        self.assertEqual(p.percentile(.75), 98)
        self.assertEqual(p.percentile(1.0), 99)

        self.assertEqual(p.max_value, 99)
        self.assertEqual(p.min_value, 94)
        self.assertEqual(p.num_values, 6)
        self.assertEqual(p.total, 94+95+96+97+98+99)
