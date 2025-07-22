import unittest
from diaspora_stream.api import Driver, Exception, TopicHandle, ThreadPool, Ordering


class TestProducer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = Driver.new("simple:libsimple-backend.so")
        cls.driver.create_topic("my_topic")
        cls.topic = cls.driver.open_topic("my_topic")

    @classmethod
    def tearDownClass(cls):
        del cls.topic
        del cls.driver

    def test_create_producer(self):
        thread_pool = self.driver.default_thread_pool
        producer = self.topic.producer(
            "my_producer",
            batch_size=32,
            max_num_batches=4,
            thread_pool=thread_pool,
            options={},
            ordering=Ordering.Loose)
        self.assertEqual(producer.name, "my_producer")
        self.assertEqual(producer.topic, self.topic)
        self.assertEqual(producer.thread_pool, thread_pool)
        self.assertEqual(producer.batch_size, 32)
        self.assertEqual(producer.max_num_batches, 4)

    def test_produce_events_without_data(self):
        producer = self.topic.producer("my_producer")
        for i in range(10):
            metadata = {"i":i}
            producer.push(metadata).wait()
        producer.flush()


if __name__ == '__main__':
    unittest.main()
