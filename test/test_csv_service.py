import unittest
from service.csv_service import CsvService as cs


class TestCsvService(unittest.TestCase):
    def test_open_excel(self):
        # test excel when path null
        self.assertIsNone(cs.open_excel(None))
        # test excel when path Empty
        self.assertIsNone(cs.open_excel(""))
        # test excel when path wrong
        self.assertIsNone(cs.open_excel("./../in/online_retailz.xlsx"))
        # test read all doc
        self.assertIsNotNone(cs.open_excel("./../in/online_retail.xlsx"))
