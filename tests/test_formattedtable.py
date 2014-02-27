from pyutil.testutil import *
from pyutil.formattedtable import *

class FormattedTableTest(TestCase):
    def test_grid(self):
        values = [
            [ "header1", "header2", "header3", ],
            [ "data1",   "data2",   "data3",   ],
            [ "data4",   "data5",   "data6",   ],
        ]

        actual_grid = tableize_grid(values[0], values[1:]).split("\n")
        expected_grid = [ x.strip() for x in """
            +---------+---------+---------+
            | header1 | header2 | header3 |
            +=========+=========+=========+
            | data1   | data2   | data3   |
            +---------+---------+---------+
            | data4   | data5   | data6   |
            +---------+---------+---------+
        """.split("\n") if x.strip() ]

        self.assertEqual(actual_grid, expected_grid)

    def test_obj_list(self):
        values = [
            { "header1" : "data1", "header2" : "data2", "header3" : "data3" },
            { "header1" : "data4", "header2" : "data5", "header3" : "data6" },
        ]

        actual_grid = tableize_obj_list([ "header1", "header2", "header3" ], values).split("\n")
        expected_grid = [ x.strip() for x in """
            +---------+---------+---------+
            | header1 | header2 | header3 |
            +=========+=========+=========+
            | data1   | data2   | data3   |
            +---------+---------+---------+
            | data4   | data5   | data6   |
            +---------+---------+---------+
        """.split("\n") if x.strip() ]

        self.assertEqual(actual_grid, expected_grid)
