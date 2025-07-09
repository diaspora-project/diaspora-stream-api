import unittest
from mochi.mofka.api import Driver, Exception, TopicHandle, ThreadPool


class TestDriver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = Driver.new("simple:libsimple-backend.so")

    @classmethod
    def tearDownClass(cls):
        del cls.driver

    def test_driver(self):
        self.assertIsInstance(self.driver, Driver)

    def test_driver_create_open_topic(self):
        self.assertFalse(self.driver.topic_exists("my_topic"))
        with self.assertRaises(Exception):
            self.driver.open_topic("my_topic")
        self.driver.create_topic("my_topic")
        self.assertTrue(self.driver.topic_exists("my_topic"))
        topic = self.driver.open_topic("my_topic")
        self.assertIsInstance(topic, TopicHandle)

    def test_driver_default_thread_pool(self):
        pool = self.driver.default_thread_pool
        self.assertIsInstance(pool, ThreadPool)

    def test_driver_make_thread_pool(self):
        pool = self.driver.make_thread_pool(1)
        self.assertIsInstance(pool, ThreadPool)


if __name__ == '__main__':
    unittest.main()
