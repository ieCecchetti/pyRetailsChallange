import unittest
from service.csv_service import CsvService


class TestCsvService(unittest.TestCase):
    def test_static_class(self):
        # static class: constructor must not be called
        with self.assertRaises(RuntimeError):
            x = CsvService()

    def test_open_excel(self):
        # test excel when path null
        self.assertIsNone(CsvService.open_excel(None))
        # test excel when path Empty
        self.assertIsNone(CsvService.open_excel(""))
        # test excel when path wrong
        self.assertIsNone(CsvService.open_excel("./../in/online_retailz.xlsx"))
        # test read all doc
        self.assertIsNotNone(CsvService.open_excel("./../in/online_retail.xlsx"))


if __name__ == '__main__':
    unittest.main()
