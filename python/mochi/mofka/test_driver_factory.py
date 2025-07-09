import unittest
from mochi.mofka.api import Driver, Exception


class TestDriverFactory(unittest.TestCase):

    def test_create_driver(self):
        driver = Driver.new("simple:libsimple-backend.so")
        self.assertIsInstance(driver, Driver)

    def test_create_driver_unknown_library(self):
        with self.assertRaises(Exception):
            driver = Driver.new("unknown:libunknown.so")

    def test_create_driver_unknown_name(self):
        with self.assertRaises(Exception):
            driver = Driver.new("unknown:libsimple-backend.so")


if __name__ == '__main__':
    unittest.main()
