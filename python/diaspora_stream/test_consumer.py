import unittest
import time
import sys
from diaspora_stream.api import Driver, Consumer, Producer, TopicHandle, Exception

class TestConsumer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = Driver.new("simple:libsimple-backend.so")

    @classmethod
    def tearDownClass(cls):
        del cls.driver

    def setUp(self):
        self.topic_name = f"my_topic_{time.time_ns()}"
        self.driver.create_topic(self.topic_name)
        self.topic = self.driver.open_topic(self.topic_name)

    def tearDown(self):
        del self.topic

    def test_create_consumer(self):
        consumer = self.topic.consumer(f"my_consumer")
        self.assertIsInstance(consumer, Consumer)
        self.assertEqual(consumer.name, "my_consumer")
        self.assertEqual(consumer.topic, self.topic)
        self.assertIsNotNone(consumer.thread_pool)
        self.assertGreater(consumer.batch_size, 0)
        self.assertGreater(consumer.max_num_batches, 0)

    def test_consume_events_iterator(self):
        producer = self.topic.producer("my_producer")
        num_events = 10
        for i in range(num_events):
            metadata = {"index": i, "value": f"event_{i}"}
            producer.push(metadata).wait()
        producer.flush()
        self.topic.mark_as_complete()

        consumer = self.topic.consumer("my_consumer")
        received_count = 0
        for event in consumer:
            if event.event_id is None:
                break
            self.assertEqual(event.event_id, received_count)
            self.assertIsNotNone(event.metadata)
            self.assertEqual(event.metadata["index"], received_count)
            self.assertEqual(event.metadata["value"], f"event_{received_count}")
            event.acknowledge()
            received_count += 1

        self.assertEqual(received_count, num_events)

    def test_consume_events_pull(self):
        producer = self.topic.producer("my_producer")
        num_events = 5
        for i in range(num_events):
            metadata = {"index": i}
            producer.push(metadata).wait()
        producer.flush()
        self.topic.mark_as_complete()

        consumer = self.topic.consumer("my_consumer")

        for i in range(num_events):
            future_event = consumer.pull()
            event = future_event.wait()
            self.assertIsNotNone(event.event_id)
            self.assertEqual(event.event_id, i)
            self.assertEqual(event.metadata["index"], i)
            event.acknowledge()

        # After all events are consumed, pull should return a NoMoreEvents event
        future_event = consumer.pull()
        event = future_event.wait()
        self.assertIsNone(event.event_id)

    def test_custom_data_broker(self):
        producer = self.topic.producer("my_producer")
        event_data = b"hello world"
        producer.push({"id": 1}, event_data).wait()
        producer.flush()
        self.topic.mark_as_complete()

        def my_selector(metadata, descriptor):
            sys.stderr.write(f"AAA {descriptor.size}\n")
            return descriptor

        def my_allocator(metadata, descriptor):
            sys.stderr.write(f"BBB {descriptor.size}\n")
            buffer = bytearray(descriptor.size)
            return [buffer]

        consumer = self.topic.consumer(
            "my_consumer",
            data_selector=my_selector,
            data_allocator=my_allocator)

        event = next(iter(consumer))
        self.assertIsNotNone(event.event_id)

        data_view = event.data
        self.assertIsInstance(data_view, list)
        self.assertEqual(len(data_view), 1)
        self.assertIsInstance(data_view[0], bytearray)
        self.assertEqual(data_view[0], event_data)


if __name__ == '__main__':
    unittest.main()
