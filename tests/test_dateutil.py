from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from wizzat.testutil import *
from wizzat.dateutil import *
import datetime, time

class TestDateUtil(TestCase):
    def test_epoch_handling(self):
        with set_millis_ctx(False):
            self.assertEqual(from_epoch(1378741939), datetime.datetime(2013, 9, 9, 15, 52, 19))
            self.assertEqual(to_epoch(datetime.datetime(2013, 9, 9, 15, 52, 19)), 1378741939)
            self.assertEqual(coerce_date(1378741939), datetime.datetime(2013, 9, 9, 15, 52, 19))
            self.assertEqual(coerce_day(1378741939), datetime.date(2013, 9, 9))

        with set_millis_ctx(True):
            self.assertEqual(from_epoch_millis(1378741939000), datetime.datetime(2013, 9, 9, 15, 52, 19))
            self.assertEqual(to_epoch_millis(datetime.datetime(2013, 9, 9, 15, 52, 19)), 1378741939000)
            self.assertEqual(coerce_date(1378741939000), datetime.datetime(2013, 9, 9, 15, 52, 19))
            self.assertEqual(coerce_day(1378741939000), datetime.date(2013, 9, 9))

    def test_now_can_be_set_and_reset(self):
        set_now(None)
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
        self.assertEqual(coerce_date(1378741939.0), datetime.datetime(2013, 9, 9, 15, 52, 19))
        self.assertEqual(coerce_date(datetime.datetime(2013, 9, 9, 15, 52, 19)), datetime.datetime(2013, 9, 9, 15, 52, 19))
        self.assertEqual(coerce_date(datetime.date(2013, 9, 9)), datetime.datetime(2013, 9, 9))

    def test_coerce_date_types(self):
        types = [
            0,
            0l,
            0.0,
            datetime.datetime.utcnow(),
            datetime.date.today(),
            "2013-03-03 00:01:02",
        ]

        for t in types:
            self.assertEqual(type(coerce_date(t)), datetime.datetime)

    def test_coerce_day_types(self):
        types = [
            0,
            0l,
            0.0,
            datetime.datetime.utcnow(),
            datetime.date.today(),
            "2013-03-03 00:01:02",
        ]

        for t in types:
            self.assertEqual(type(coerce_day(t)), datetime.date)

    def test_ushort_to_day(self):
        self.assertEqual(ushort_to_day(0), datetime.date(1970, 1, 1))
        self.assertEqual(ushort_to_day(10000), datetime.date(1970, 1, 1) + days(10000))
        self.assertEqual(ushort_to_day(65535), datetime.date(1970, 1, 1) + days(65535))
        self.assertEqual(ushort_to_day(65535*2), datetime.date(1970, 1, 1) + days(65535*2))

    def test_day_to_ushort(self):
        self.assertEqual(0,       day_to_ushort(datetime.date(1970, 1, 1)))
        self.assertEqual(10000,   day_to_ushort(datetime.date(1970, 1, 1) + days(10000)))
        self.assertEqual(65535,   day_to_ushort(datetime.date(1970, 1, 1) + days(65535)))
        self.assertEqual(65535*2, day_to_ushort(datetime.date(1970, 1, 1) + days(65535*2)))

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

    def test_to_local_tz(self):
        set_local_tz("US/Pacific")

        # Daylight savings begins 2014-03-09 02:00:00
        self.assertEqual(to_local_tz(coerce_date("2014-03-09 09:00:00")), coerce_date("2014-03-09 01:00:00"))
        self.assertEqual(to_local_tz(coerce_date("2014-03-09 09:59:59")), coerce_date("2014-03-09 01:59:59"))
        self.assertEqual(to_local_tz(coerce_date("2014-03-09 10:00:00")), coerce_date("2014-03-09 03:00:00"))

        # Daylight savings ends 2014-11-02 02:00:00
        self.assertEqual(to_local_tz(coerce_date("2014-11-02 08:00:00")), coerce_date("2014-11-02 01:00:00"))
        self.assertEqual(to_local_tz(coerce_date("2014-11-02 08:59:59")), coerce_date("2014-11-02 01:59:59"))
        self.assertEqual(to_local_tz(coerce_date("2014-11-02 09:00:00")), coerce_date("2014-11-02 01:00:00"))
        self.assertEqual(to_local_tz(coerce_date("2014-11-02 10:00:00")), coerce_date("2014-11-02 02:00:00"))

    def test_from_local_tz(self):
        set_local_tz("US/Pacific")

        # Daylight savings begins 2014-03-09 02:00:00
        self.assertEqual(from_local_tz(coerce_date("2014-03-09 01:00:00")), coerce_date("2014-03-09 09:00:00"))
        self.assertEqual(from_local_tz(coerce_date("2014-03-09 01:59:59")), coerce_date("2014-03-09 09:59:59"))
        self.assertEqual(from_local_tz(coerce_date("2014-03-09 03:00:00")), coerce_date("2014-03-09 10:00:00"))

        # Daylight savings ends 2014-11-02 02:00:00
        self.assertEqual(from_local_tz(coerce_date("2014-11-02 01:00:00")), coerce_date("2014-11-02 09:00:00"))
        self.assertEqual(from_local_tz(coerce_date("2014-11-02 01:59:59")), coerce_date("2014-11-02 09:59:59"))
        self.assertEqual(from_local_tz(coerce_date("2014-11-02 02:00:00")), coerce_date("2014-11-02 10:00:00"))
