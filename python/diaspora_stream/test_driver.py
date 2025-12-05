import unittest
import os
import json
from diaspora_stream.api import Driver, Exception, TopicHandle, ThreadPool


class TestDriver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        backend = os.environ.get("DIASPORA_TEST_BACKEND", "simple:libdiaspora-simple-backend.so")
        backend_args = json.loads(os.environ.get("DIASPORA_TEST_BACKEND_ARGS", "{}"))
        cls.driver = Driver(backend=backend, options=backend_args)

    @classmethod
    def tearDownClass(cls):
        del cls.driver

    def test_driver(self):
        self.assertIsInstance(self.driver, Driver)

    def test_driver_create_open_topic(self):
        topic_args = json.loads(os.environ.get("DIASPORA_TEST_TOPIC_ARGS", "{}"))
        self.assertFalse(self.driver.topic_exists("my_topic"))
        with self.assertRaises(Exception):
            self.driver.open_topic("my_topic")
        self.driver.create_topic("my_topic", options=topic_args)
        self.assertTrue(self.driver.topic_exists("my_topic"))
        topic = self.driver.open_topic("my_topic")
        self.assertIsInstance(topic, TopicHandle)

    def test_driver_default_thread_pool(self):
        pool = self.driver.default_thread_pool
        self.assertIsInstance(pool, ThreadPool)

    def test_driver_make_thread_pool(self):
        pool = self.driver.make_thread_pool(1)
        self.assertIsInstance(pool, ThreadPool)
        del pool


if __name__ == '__main__':
    unittest.main()
