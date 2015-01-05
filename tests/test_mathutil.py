import math
from wizzat.testutil import *
from wizzat.mathutil import *

class AverageTest(TestCase):
    pass

class PercentileTest(TestCase):
    def test_percentile__100(self):
        p = Percentile(*xrange(100))
        self.assertEqual(p.percentile(0.0), 0)
        self.assertEqual(p.percentile(.25), 24)
        self.assertEqual(p.percentile(0.5), 48)
        self.assertEqual(p.percentile(.75), 74)
        self.assertEqual(p.percentile(1.0), 100)

    def test_percentile__1000(self):
        p = Percentile(*xrange(1000))
        self.assertEqual(p.percentile(0.0), 0)
        self.assertEqual(p.percentile(.25), 247)
        self.assertEqual(p.percentile(0.5), 494)
        self.assertEqual(p.percentile(.75), 748)
        self.assertEqual(p.percentile(1.0), 1009)

    def test_percentile_deviation(self):
        r = range(0, 2**64, 2**64/1000000)
        p = Percentile(*(float(x) for x in r))

        for x in xrange(100):
            real_pct = r[int((x/100.0) * len(r))]
            pct = p.percentile(x/100.0)

            if real_pct != pct:
                variance = 1.0-math.fabs(1.0 * real_pct / pct)
                self.assertTrue(variance < 0.055, "{} != {}, {} {}".format(pct, real_pct, variance, x))

    def test_handles_up_to_64_bit_integers(self):
        Percentile(2**64)

        with self.assertRaises(IndexError):
            Percentile(2**67)

        with self.assertRaises(IndexError):
            Percentile(2**128)

    def test_handles_zero(self):
        p = Percentile(0, 0, 0, 1)
        self.assertEqual(p.min_idx, 0)
        self.assertEqual(p.max_idx, 30)

        self.assertEqual(p.percentile(0.0), 0.0)
        self.assertEqual(p.percentile(.98), 1.0)
        self.assertEqual(p.percentile(1.0), 1.0)

    def test_empty(self):
        p = Percentile()
        self.assertEqual(p.percentile(0),   None)
        self.assertEqual(p.percentile(.25), None)
        self.assertEqual(p.percentile(.50), None)
        self.assertEqual(p.percentile(.75), None)
        self.assertEqual(p.percentile(.98), None)
        self.assertEqual(p.percentile(1.0), None)
