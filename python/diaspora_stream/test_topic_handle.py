import unittest
from diaspora_stream.api import Driver, Exception, TopicHandle, ThreadPool


class TestTopicHandle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = Driver.new("simple:libsimple-backend.so")
        cls.driver.create_topic("my_topic")
        cls.topic = cls.driver.open_topic("my_topic")

    @classmethod
    def tearDownClass(cls):
        del cls.topic
        del cls.driver

    def test_topic_handle(self):
        self.assertIsInstance(self.topic, TopicHandle)
        self.assertEqual(self.topic.name, "my_topic")
        partitions = self.topic.partitions
        self.assertEqual(len(partitions), 1)

if __name__ == '__main__':
    unittest.main()
