import unittest
import os
import json
from diaspora_stream.api import Driver, Exception


class TestDriverFactory(unittest.TestCase):

    def test_create_driver(self):
        backend = os.environ.get("DIASPORA_TEST_BACKEND", "simple:libsimple-backend.so")
        backend_args = json.loads(os.environ.get("DIASPORA_TEST_BACKEND_ARGS", "{}"))
        driver = Driver.new(backend, metadata=backend_args)
        self.assertIsInstance(driver, Driver)

    def test_create_driver_unknown_library(self):
        with self.assertRaises(Exception):
            driver = Driver.new("unknown:libunknown.so")

    def test_create_driver_unknown_name(self):
        with self.assertRaises(Exception):
            driver = Driver.new("unknown:libsimple-backend.so")


if __name__ == '__main__':
    unittest.main()
