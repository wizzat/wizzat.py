import unittest
from pyutil.testutil import *
from pyutil.dateutil import *

class TestDateUtil(unittest.TestCase):
    def test_epoch_handling(self):
        self.assertEqual(from_epoch(1378741939), datetime.datetime(2013, 9, 9, 8, 52, 00))
        self.assertEqual(to_epoch(datetime.datetime(2013, 9, 9, 8, 52, 00)), 1378741939)

    @skip_unfinished
    def test_now_can_be_set_and_reset(self):
        pass

    @skip_unfinished
    def test_coerce_date(self):
        pass

    @skip_unfinished
    def test_to_second(self):
        pass

    @skip_unfinished
    def test_to_minute(self):
        pass

    @skip_unfinished
    def test_to_hour(self):
        pass

    @skip_unfinished
    def test_to_day(self):
        pass

    @skip_unfinished
    def test_to_week(self):
        pass

    @skip_unfinished
    def test_to_month(self):
        pass

    @skip_unfinished
    def test_to_quarter(self):
        pass

    @skip_unfinished
    def test_to_year(self):
        pass
