from pyutil.testutil import *
from pyutil.dateutil import *
import datetime, time

class TestDateUtil(TestCase):
    def test_epoch_handling(self):
        self.assertEqual(from_epoch(1378741939), datetime.datetime(2013, 9, 9, 15, 52, 19))
        self.assertEqual(to_epoch(datetime.datetime(2013, 9, 9, 15, 52, 19)), 1378741939)

    def test_now_can_be_set_and_reset(self):
        t1 = now()
        time.sleep(0.1)
        t2 = now()
        self.assertNotEqual(t1, t2)

        set_now(now())
        t1 = now()
        time.sleep(0.1)
        t2 = now()
        self.assertEqual(t1, t2)

        t3 = now()
        reset_now()
        t1 = now()
        time.sleep(0.1)
        t2 = now()
        self.assertEqual(t1, t2)
        self.assertNotEqual(t1, t3)

    def test_coerce_date(self):
        self.assertEqual(coerce_date(1378741939), datetime.datetime(2013, 9, 9, 15, 52, 19))
        self.assertEqual(coerce_date(datetime.datetime(2013, 9, 9, 15, 52, 19)), datetime.datetime(2013, 9, 9, 15, 52, 19))

    def test_coerce_date__string_formatting(self):
        register_date_format("%Y-%m-%d %H:%M:%S")
        self.assertEqual(coerce_date("2013-09-09 15:52:19"), datetime.datetime(2013, 9, 9, 15, 52, 19))

    def test_to_second(self):
        self.assertEqual(to_second(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 9, 15, 52, 19, 0))
        self.assertEqual(to_second(datetime.datetime(2013, 9, 9, 15, 52, 19, 99999)), datetime.datetime(2013, 9, 9, 15, 52, 19, 0))
        self.assertEqual(to_second(datetime.datetime(2013, 9, 9, 15, 52, 20)), datetime.datetime(2013, 9, 9, 15, 52, 20, 0))

    def test_to_minute(self):
        self.assertEqual(to_minute(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 9, 15, 52, 0, 0))

    def test_to_hour(self):
        self.assertEqual(to_hour(datetime.datetime(2013, 9, 9, 15, 0, 0, 0)), datetime.datetime(2013, 9, 9, 15, 0, 0, 0))
        self.assertEqual(to_hour(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 9, 15, 0, 0, 0))
        self.assertEqual(to_hour(datetime.datetime(2013, 9, 9, 15, 59, 59, 99999)), datetime.datetime(2013, 9, 9, 15, 0, 0, 0))

    def test_to_day(self):
        self.assertEqual(to_day(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 9, 0, 0, 0, 0))

    def test_to_week(self):
        self.assertEqual(to_week(datetime.datetime(2013, 9, 8, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 2, 0, 0, 0, 0)) # Sun
        self.assertEqual(to_week(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 9, 0, 0, 0, 0)) # Mon

    def test_to_month(self):
        self.assertEqual(to_month(datetime.datetime(2013, 9, 8, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 1, 0, 0, 0, 0))
        self.assertEqual(to_month(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 9, 1, 0, 0, 0, 0))

    def test_to_quarter(self):
        self.assertEqual(to_quarter(datetime.datetime(2013, 1, 1, 0, 0, 0)),            datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 1, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 2, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 3, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 3, 30, 23, 59, 59, 99999)), datetime.datetime(2013, 1, 1, 0, 0, 0, 0))

        self.assertEqual(to_quarter(datetime.datetime(2013, 4, 1, 0, 0, 0)),            datetime.datetime(2013, 4, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 4, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 4, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 5, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 4, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 6, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 4, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 4, 30, 23, 59, 59, 99999)), datetime.datetime(2013, 4, 1, 0, 0, 0, 0))

        self.assertEqual(to_quarter(datetime.datetime(2013, 7, 1, 0, 0, 0)),            datetime.datetime(2013, 7, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 7, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 7, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 8, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 7, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 7, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 9, 30, 23, 59, 59, 99999)), datetime.datetime(2013, 7, 1, 0, 0, 0, 0))

        self.assertEqual(to_quarter(datetime.datetime(2013, 10, 1, 0, 0, 0)),            datetime.datetime(2013, 10, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 10, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 10, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 11, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 10, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 12, 9, 15, 52, 19, 43435)),  datetime.datetime(2013, 10, 1, 0, 0, 0, 0))
        self.assertEqual(to_quarter(datetime.datetime(2013, 12, 31, 23, 59, 59, 99999)), datetime.datetime(2013, 10, 1, 0, 0, 0, 0))

    def test_to_year(self):
        self.assertEqual(to_year(datetime.datetime(2013, 9, 8, 15, 52, 19, 43435)), datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_year(datetime.datetime(2013, 9, 9, 15, 52, 19, 43435)), datetime.datetime(2013, 1, 1, 0, 0, 0, 0))

        self.assertEqual(to_year(datetime.datetime(2013, 12, 31, 23, 59, 59, 99999)), datetime.datetime(2013, 1, 1, 0, 0, 0, 0))
        self.assertEqual(to_year(datetime.datetime(2014, 1, 1, 0, 0, 0)), datetime.datetime(2014, 1, 1, 0, 0, 0, 0))
