from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import difflib
import wizzat.testutil
import wizzat.textutil

class TextUtilTest(wizzat.testutil.TestCase):
    def assertTextTable(self, actual, s):
        expected = [ x.strip() for x in s.split('\n') if x.strip() ]
        actual = [ x.strip() for x in actual.split('\n') if x.strip() ]
        diff = list(difflib.unified_diff(expected, actual, n=20))

        if diff:
            raise AssertionError("Assert JSON failed:\n{diff}".format(
                diff = '\n'.join(diff),
            ))

    def test_text_table__lists(self):
        actual_grid = wizzat.textutil.text_table(
            [ "header1", "header2", "header3", ], [
            [ "data1",   "data2aaa",   "data3",   ],
            [ "data4",   "data5",   "data6",   ],
        ])

        self.assertTextTable(actual_grid, """
            +---------+----------+---------+
            | header1 | header2  | header3 |
            +=========+==========+=========+
            | data1   | data2aaa | data3   |
            +---------+----------+---------+
            | data4   | data5    | data6   |
            +---------+----------+---------+
        """)

    def test_text_table__dicts(self):
        actual_grid = wizzat.textutil.text_table(
            [ "header1", "header2", "header3", ], [
            { "header1" : "data1", "header2" : "data2", "header3" : "data3" },
            { "header1" : "data4", "header2" : "data5", "header3" : "data6" },
        ], row_dicts = True)

        self.assertTextTable(actual_grid, """
            +---------+---------+---------+
            | header1 | header2 | header3 |
            +=========+=========+=========+
            | data1   | data2   | data3   |
            +---------+---------+---------+
            | data4   | data5   | data6   |
            +---------+---------+---------+
        """)
