import unittest
from service.utils import Utils


class TestUtils(unittest.TestCase):
    def test_static_class(self):
        # static class: constructor must not be called
        with self.assertRaises(RuntimeError):
            x = Utils()

    def test_open_excel(self):
        # test obtained default item are 5
        with self.assertRaises(RuntimeError):
            Utils.get_n_colors(None)
        # test obtained default item are 5
        colors = Utils.get_n_colors()
        self.assertIsNotNone(colors)
        self.assertEqual(5, len(colors))
        # check normal working process
        expected = 7
        colors = Utils.get_n_colors(expected)
        self.assertIsNotNone(colors)
        self.assertEqual(expected, len(colors))


if __name__ == '__main__':
    unittest.main()
