#define PYBIND11_DETAILED_ERROR_MESSAGES
#define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"
#include <mofka/DataView.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/Driver.hpp>
#include "../src/JsonUtil.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

namespace mofka {

struct PythonBindingHelper {

    template<typename T>
    static auto GetSelf(const T& x) {
        return x.self;
    }

    template<typename T, typename I>
    static auto FromInterface(std::shared_ptr<I> impl) {
        return T{std::move(impl)};
    }
};

}

static auto get_buffer_info(const py::buffer& buf) {
    return buf.request();
}

static void check_buffer_is_contiguous(const py::buffer_info& buf_info) {
    if (!(PyBuffer_IsContiguous((buf_info).view(), 'C')
       || PyBuffer_IsContiguous((buf_info).view(), 'F')))
        throw mofka::Exception("Non-contiguous Python buffers are not yet supported");
}

static void check_buffer_is_writable(const py::buffer_info& buf_info) {
    if(buf_info.readonly) throw mofka::Exception("Python buffer is read-only");
}

static auto data_helper(const py::buffer& buffer) {
    auto buffer_info = get_buffer_info(buffer);
    check_buffer_is_contiguous(buffer_info);
    auto owner = new py::object(std::move(buffer));
    auto free_cb = [owner](mofka::DataView::UserContext) { delete owner; };
    return mofka::DataView(buffer_info.ptr, buffer_info.size, owner, std::move(free_cb));
}

static auto data_helper(const py::list& buffers) {
    std::vector<mofka::DataView::Segment> segments;
    segments.reserve(buffers.size());
    for (auto& buff : buffers){
        auto buff_info = get_buffer_info(buff.cast<py::buffer>());
        check_buffer_is_contiguous(buff_info);
        segments.push_back(
            mofka::DataView::Segment{
                buff_info.ptr,
                static_cast<size_t>(buff_info.size)});
    }
    auto owner = new py::object(std::move(buffers));
    auto free_cb = [owner](mofka::DataView::UserContext) { delete owner; };
    return mofka::DataView(std::move(segments), owner, std::move(free_cb));
}

using PythonDataSelector = std::function<std::optional<mofka::DataDescriptor>(const nlohmann::json&, const mofka::DataDescriptor&)>;
using PythonDataBroker   = std::function<py::list(const nlohmann::json&, const mofka::DataDescriptor&)>;

struct AbstractDataOwner {
    virtual py::object toPythonObject() const = 0;
    virtual ~AbstractDataOwner() = default;
};

struct __attribute__ ((visibility("hidden"))) PythonDataOwner : public AbstractDataOwner {
    py::object m_obj;

    PythonDataOwner(py::object obj)
    : m_obj{std::move(obj)} {}

    py::object toPythonObject() const override {
        return m_obj;
    }
};

struct BufferDataOwner : public AbstractDataOwner {
    std::vector<char> m_data;

    BufferDataOwner(size_t size)
    : m_data(size) {}

    py::object toPythonObject() const override {
        py::list result;
        result.append(py::memoryview::from_memory(m_data.data(), m_data.size()));
        return result;
    }
};

PYBIND11_MODULE(pymofka_api, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::register_exception<mofka::Exception>(m, "Exception", PyExc_RuntimeError);

    m.attr("AdaptiveBatchSize") = py::int_(mofka::BatchSize::Adaptive().value);

    py::class_<mofka::ValidatorInterface,
               std::shared_ptr<mofka::ValidatorInterface>>(m, "Validator")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PythonBindingHelper::GetSelf(
                    mofka::Validator::FromMetadata(md));
            }, R"(
            Create a Validator instance from some Metadata. The metadata argument
            is expected to be a dictionary object with at least a "type" field.
            The type can be in the form "name:library.so" if library.so must be
            loaded to access the Validator. If the "type" field is not provided,
            it is assumed to be "default".

            Parameters
            ----------

            metadata (dict): metadata to pass to the Validator factory.

            Returns
            -------

            New Validator instance.
            )",
            "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::ThreadPoolInterface,
               std::shared_ptr<mofka::ThreadPoolInterface>>(m, "ThreadPool")
        .def_property_readonly("thread_count",
            [](const mofka::ThreadPoolInterface& thread_pool) -> std::size_t {
                return thread_pool.threadCount().count;
            }, "Returns the number of underlying threads.")
    ;

    py::enum_<mofka::Ordering>(m, "Ordering")
        .value("Strict", mofka::Ordering::Strict)
        .value("Loose", mofka::Ordering::Loose)
    ;

    py::class_<mofka::SerializerInterface,
               std::shared_ptr<mofka::SerializerInterface>>(m, "Serializer")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PythonBindingHelper::GetSelf(
                    mofka::Serializer::FromMetadata( md));
            }, R"(
            Create a Serializer instance from some Metadata. The metadata argument
            is expected to be a dictionary object with at least a "type" field.
            The type can be in the form "name:library.so" if library.so must be
            loaded to access the Serializer. If the "type" field is not provided,
            it is assumed to be "default".

            Parameters
            ----------

            metadata (dict): metadata to pass to the Serializer factory.

            Returns
            -------

            New Serializer instance.
            )",
            "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::PartitionSelectorInterface,
               std::shared_ptr<mofka::PartitionSelectorInterface>>(m, "PartitionSelector")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PythonBindingHelper::GetSelf(
                    mofka::PartitionSelector::FromMetadata(md));
            }, R"(
            Create a PartitionSelector instance from some Metadata. The metadata argument
            is expected to be a dictionary object with at least a "type" field.
            The type can be in the form "name:library.so" if library.so must be
            loaded to access the PartitionSelector. If the "type" field is not provided,
            it is assumed to be "default".

            Parameters
            ----------

            metadata (dict): metadata to pass to the PartitionSelector factory.

            Returns
            -------

            New PartitionSelector instance.
            )",
            "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::DriverInterface,
               std::shared_ptr<mofka::DriverInterface>>(m, "Driver")
        .def("create_topic",
             [](mofka::DriverInterface& driver,
                std::string_view name,
                const nlohmann::json& options,
                std::shared_ptr<mofka::ValidatorInterface> validator,
                std::shared_ptr<mofka::PartitionSelectorInterface> partition_selector,
                std::shared_ptr<mofka::SerializerInterface> serializer) {
                    driver.createTopic(name, options, validator, partition_selector, serializer);
             },
             R"(
             Create a topic with a given name, if it does not exist yet.

             Parameters
             ----------

             name (str): Name of the topic to create.
             options (dict): Backend-specific options.
             validator (Validator): Validator instance to call when pushing events.
             partition_selector (PartitionSelector): PartitionSelector to call to choose a partition.
             serializer (Serializer): Serializer to call to serialize events metadata.
             )",
             "name"_a, py::kw_only(), "options"_a=nlohmann::json::object(),
             "validator"_a=mofka::PythonBindingHelper::GetSelf(mofka::Validator{}),
             "partition_selector"_a=mofka::PythonBindingHelper::GetSelf(mofka::PartitionSelector{}),
             "serializer"_a=mofka::PythonBindingHelper::GetSelf(mofka::Serializer{}))
        .def("open_topic",
             &mofka::DriverInterface::openTopic,
             R"(
             Open a topic with a given its name.

             Parameters
             ----------

             name (str): Name of the topic to open.

             Returns
             -------

             A TopicHandle instance representing the opened topic.
             )",
             "name"_a)
        .def("topic_exists",
             &mofka::DriverInterface::topicExists,
             R"(
             Check if a topic with a given name exists.

             Parameters
             ----------

             name (str): Name of the topic.

             Returns
             -------

             True if the topic exists, False otherwise.
             )",
             "name"_a)
        .def("make_thread_pool",
             [](const mofka::DriverInterface& driver, size_t count) {
                return driver.makeThreadPool(mofka::ThreadCount{count});
                }, R"(
             Create a ThreadPool instance with the given number of threads.

             Parameters
             ----------

             count (int): Number of threads.

             Returns
             -------

             A ThreadPool instance.
             )",
             "count"_a)
        .def_property_readonly("default_thread_pool",
             &mofka::DriverInterface::defaultThreadPool,
             R"(
             Get the default ThreadPool of the Driver.

             Returns
             -------

             A ThreadPool instance.
             )")
        .def_static("new",
            [](const std::string& name, const nlohmann::json& md) {
                return mofka::DriverFactory::create(name, mofka::Metadata{md});
            },
            R"(
            Create a new Driver from a name an some backend-specific metadata.
            The name can be in the "backend" if the backend has already been
            loaded into the process' memory. Using "backend:library.so" will cause
            the factory to dlopen library.so before searching for the "backend".

            Parameters
            ----------

            name (str): Name of the backend.
            metadata (dict): Backend-specific configuration.

            Returns
            -------

            A Driver instance.
            )"
            "name"_a, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::TopicHandleInterface,
               std::shared_ptr<mofka::TopicHandleInterface>>(m, "TopicHandle")
        .def_property_readonly(
                "name",
                &mofka::TopicHandleInterface::name,
                "Name of the topic.")
        .def_property_readonly("partitions",
            [](const mofka::TopicHandleInterface& topic) {
                std::vector<nlohmann::json> result;
                for(auto& p : topic.partitions()) {
                    result.push_back(p.json());
                }
                return result;
            }, "List of partitions of the topic.")
        .def("mark_as_complete",
             &mofka::TopicHandleInterface::markAsComplete,
             R"(
             Closes the topic, preventing new events from being pushed into it,
             and notifying consumers that no new events are to be expected.
             )")
        .def("producer",
            [](mofka::TopicHandleInterface& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               mofka::Ordering ordering,
               std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
               const nlohmann::json& options) {
                return topic.makeProducer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxNumBatches{max_batch}, ordering, thread_pool,
                    mofka::Metadata{options});
            },
            R"(
            Create a Producer instance to produce events in this topic.

            Parameters
            ----------

            name (str): Name of the producer.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of pending batches before push becomes blocking.
            ordering (Ordering): type of ordering (strict or loose).
            thread_pool (ThreadPool): ThreadPool to use for any work needed for producing the events.
            options (dict): Backend-specific options.

            Returns
            -------

            A Producer instance.
            )",
            "name"_a="", py::kw_only(),
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "ordering"_a=mofka::Ordering::Strict,
            "thread_pool"_a=std::shared_ptr<mofka::ThreadPoolInterface>{},
            "options"_a=nlohmann::json(nullptr))
        .def("consumer",
            [](mofka::TopicHandleInterface& topic,
               std::string_view name,
               PythonDataSelector selector,
               PythonDataBroker broker,
               std::size_t batch_size,
               std::size_t max_batch,
               std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
               std::optional<std::vector<size_t>> targets,
               const nlohmann::json& options) {
                auto cpp_broker = broker ?
                    [broker=std::move(broker)]
                    (const mofka::Metadata& metadata,
                     const mofka::DataDescriptor& descriptor) -> mofka::DataView {
                        auto segments = broker(metadata.json(), descriptor);
                        std::vector<mofka::DataView::Segment> cpp_segments;
                        cpp_segments.reserve(segments.size());
                        for(auto& segment : segments) {
                            auto buf_info = get_buffer_info(segment.cast<py::buffer>());
                            check_buffer_is_writable(buf_info);
                            check_buffer_is_contiguous(buf_info);
                            cpp_segments.push_back({buf_info.ptr, (size_t)buf_info.size});
                        }
                        auto owner = new PythonDataOwner{std::move(segments)};
                        auto free_cb = [owner](mofka::DataView::UserContext) { delete owner; };
                        auto data = mofka::DataView{std::move(cpp_segments), owner, std::move(free_cb)};
                        return data;
                }
                : mofka::DataBroker{};
                auto cpp_selector = selector ?
                    [selector=std::move(selector)]
                    (const mofka::Metadata& metadata,
                     const mofka::DataDescriptor& descriptor) -> mofka::DataDescriptor {
                        std::optional<mofka::DataDescriptor> result = selector(metadata.json(), descriptor);
                        if(result) return result.value();
                        else return mofka::DataDescriptor();
                    }
                : mofka::DataSelector{};
                std::vector<size_t> default_targets;
                return topic.makeConsumer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxNumBatches{max_batch},
                    thread_pool,
                    mofka::DataBroker{cpp_broker},
                    mofka::DataSelector{cpp_selector},
                    targets.value_or(default_targets),
                    mofka::Metadata{options});
               },
            R"(
            Create a Consumer instance to consume events from this topic.

            The data_selector argument must be a callable taking a dictionary (an event's metadata)
            and a DataDescriptor as arguments, and returning a DataDescriptor representing the subset
            of the event's data to load.

            The data_broker argument must be a callable taking a dictionary (an event's metadata)
            and a DataDescriptor as arguments, and returning a list of object satisfying the buffer
            protocol (e.g. bytearray, memoryview, numpy array, etc.).

            Parameters
            ----------

            name (str): Name of the consumer.
            data_selector (Callable[Optional[DataDescriptor], [dict, DataDescriptor]]): data selector.
            data_broker (Callable[list, [dict, DataDescriptor]]): data broker.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of batches to prefetch at any time.
            thread_pool (ThreadPool): ThreadPool to use for any work needed for consuming the events.
            targets (Optional[list[int]): List of indices of partitions to consume from.
            options (dict): Backend-specific options.

            Returns
            -------

            A Consumer instance.
            )",
            "name"_a, py::kw_only(),
            "data_selector"_a, "data_broker"_a,
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "thread_pool"_a=mofka::PythonBindingHelper::GetSelf(mofka::ThreadPool{}),
            "targets"_a=std::optional<std::vector<size_t>>{},
            "options"_a=nlohmann::json(nullptr))
        .def("consumer",
            [](mofka::TopicHandleInterface& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
               std::optional<std::vector<size_t>> targets,
               const nlohmann::json& options) {
                auto cpp_broker = [](const mofka::Metadata& metadata,
                                     const mofka::DataDescriptor& descriptor) -> mofka::DataView {
                        (void)metadata;
                        auto owner = new BufferDataOwner{descriptor.size()};
                        std::vector<mofka::DataView::Segment> cpp_segment{
                            mofka::DataView::Segment{owner->m_data.data(), owner->m_data.size()}
                        };
                        auto free_cb = [owner](mofka::DataView::UserContext) { delete owner; };
                        auto data = mofka::DataView{std::move(cpp_segment), owner, std::move(free_cb)};
                        return data;
                };
                auto cpp_selector = [](const mofka::Metadata& metadata,
                                       const mofka::DataDescriptor& descriptor) -> mofka::DataDescriptor {
                        (void)metadata;
                        return descriptor;
                };
                std::vector<size_t> default_targets;
                return topic.makeConsumer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxNumBatches{max_batch}, thread_pool,
                    mofka::DataBroker{cpp_broker},
                    mofka::DataSelector{cpp_selector},
                    targets.value_or(default_targets),
                    mofka::Metadata{options});
               },
            R"(
            Create a Consumer instance to consume events from this topic.
            With version of the consumer function returns a consumer that always loads all
            of an event's data, and returns it as an internal buffer, of which the returned
            event's data method will return a memoryview.

            Parameters
            ----------

            name (str): Name of the consumer.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of batches to prefetch at any time.
            thread_pool (ThreadPool): ThreadPool to use for any work needed for consuming the events.
            targets (Optional[list[int]): List of indices of partitions to consume from.
            options (dict): Backend-specific options.

            Returns
            -------

            A Consumer instance.
            )",
            "name"_a, py::kw_only(),
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "thread_pool"_a=mofka::PythonBindingHelper::GetSelf(mofka::ThreadPool{}),
            "targets"_a=std::optional<std::vector<size_t>>{},
            "options"_a=nlohmann::json(nullptr))
    ;

    py::class_<mofka::ProducerInterface,
               std::shared_ptr<mofka::ProducerInterface>>(m, "Producer")
        .def_property_readonly("name", &mofka::ProducerInterface::name, "Name of the producer.")
        .def_property_readonly("thread_pool", &mofka::ProducerInterface::threadPool,
                               "ThreadPool used by the producer.")
        .def_property_readonly("topic", &mofka::ProducerInterface::topic,
                               "TopicHandle of the topic this producer produces to.")
        .def("push",
            [](mofka::ProducerInterface& producer,
               std::string metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a memoryview or any object satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a string.
            data (memoryview): Data part of the event (single memoryview).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               nlohmann::json metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a memoryview or any object satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (dict): Metadata part of the event, as a dictionary.
            data (memoryview): Data part of the event (single memoryview).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               std::string metadata,
               const py::list& b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a list of memoryviews or any objects satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a string.
            data (list[memoryview]): Data part of the event (list of memoryviews).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=std::vector<py::memoryview>{},
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               nlohmann::json metadata,
               py::list b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a list of memoryviews or any objects satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a dictionary.
            data (list[memoryview]): Data part of the event (list of memoryviews).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("flush", &mofka::ProducerInterface::flush,
             "Flush the producer, i.e. ensure all the events previously pushed have been sent.")
        .def_property_readonly("batch_size",
            [](const mofka::ProducerInterface& producer) -> std::size_t {
                return producer.batchSize().value;
            }, "Return the batch size of the producer.")
        .def_property_readonly("max_num_batches",
            [](const mofka::ProducerInterface& producer) -> std::size_t {
                return producer.maxNumBatches().value;
            }, "Return the maximum number of pending batches in the producer.")
    ;

    py::class_<mofka::ConsumerInterface,
               std::shared_ptr<mofka::ConsumerInterface>>(m, "Consumer")
        .def_property_readonly("name", &mofka::ConsumerInterface::name,
                               "Name of the consumer.")
        .def_property_readonly("thread_pool", &mofka::ConsumerInterface::threadPool,
                               "ThreadPool used by the consumer.")
        .def_property_readonly("topic", &mofka::ConsumerInterface::topic,
                               "TopicHandle of the topic from which this consumer receives events.")
        .def_property_readonly("data_broker", &mofka::ConsumerInterface::dataBroker,
                               "Callable used to allocate memory for the data part of events.")
        .def_property_readonly("data_selector", &mofka::ConsumerInterface::dataSelector,
                               "Callable used to select the part of the data to load.")
        .def_property_readonly("batch_size",
            [](const mofka::ConsumerInterface& consumer) -> std::size_t {
                return consumer.batchSize().value;
            }, "Bath size used by the consumer.")
        .def_property_readonly("max_num_batches",
            [](const mofka::ConsumerInterface& consumer) -> std::size_t {
                return consumer.maxNumBatches().value;
            }, "Maximum number of bathes used by the consumer internally.")
        .def("pull", &mofka::ConsumerInterface::pull, R"(
            Pull an event. This function will immediately return a FutureEvent.
            Calling wait() on this future will block until an event is available.

            Returns
            -------

            A FutureEvent instance.
        )")
        .def("process",
            [](mofka::ConsumerInterface& consumer,
               mofka::EventProcessor processor,
               std::shared_ptr<mofka::ThreadPoolInterface> threadPool,
               std::size_t maxEvents) {
                return consumer.process(processor, threadPool, mofka::NumEvents{maxEvents});
               },
            R"(
                Process the incoming events using the provided callable.

                Parameters
                ----------

                processor (Callable[None, Event]): Callable to use to process the events.
                thread_pool (ThreadPool): ThreadPool to use to submit calls to the processor.
                max_events (int): Maximum number of events to process.
            )",
            "processor"_a, py::kw_only(),
            "thread_pool"_a=std::shared_ptr<mofka::ThreadPoolInterface>{},
            "max_events"_a=std::numeric_limits<size_t>::max()
            )
        .def("__iter__",
             [](std::shared_ptr<mofka::ConsumerInterface> consumer) {
                auto c = mofka::PythonBindingHelper::FromInterface<mofka::Consumer>(consumer);
                return py::make_iterator(c.begin(), c.end());
              }, R"(
                Create an iterator to iterate over the events.

                Returns
                -------

                A Python iterator.
            )",
            py::keep_alive<0, 1>())
    ;

    py::class_<mofka::DataDescriptor>(m, "DataDescriptor")
        .def(py::init<>())
        .def(py::init<std::string_view, size_t>())
        .def_property_readonly("size", &mofka::DataDescriptor::size,
                               "Amount of data (in bytes) this DataDescriptor represents.")
        .def_property_readonly("location",
            py::overload_cast<>(&mofka::DataDescriptor::location, py::const_),
            "Opaque (backend-specific) location string.")
        .def("make_stride_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t numblocks,
               std::size_t blocksize,
               std::size_t gapsize) -> mofka::DataDescriptor {
                return data_descriptor.makeStridedView(offset, numblocks, blocksize, gapsize);
            }, R"(
            Construct a new DataDescriptor by taking a strided selection of the current DataDescriptor.

            Parameters
            ----------

            offset (int): Offset at which to start the selection.
            num_blocks (int): Number of blocks.
            block_size (int): Size of each block.
            gat_size (int): Number of bytes to leave out between each block.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "offset"_a,
            "num_blocks"_a,
            "block_size"_a,
            "gap_size"_a)
        .def("make_sub_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t size) -> mofka::DataDescriptor {
                return data_descriptor.makeSubView(offset, size);
            }, R"(
            Construct a new DataDescriptor by taking a sub-selection of the current DataDescriptor.

            Parameters
            ----------

            offset (int): Offset at which to start the selection.
            size (int): Size of the selection.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "offset"_a, "size"_a)
        .def("make_unstructured_view",
            [](const mofka::DataDescriptor& data_descriptor,
               const std::vector<std::pair<std::size_t, std::size_t>> segments) -> mofka::DataDescriptor {
                std::vector<mofka::DataDescriptor::Segment> seg;
                seg.reserve(segments.size());
                for(auto& s : segments) seg.push_back(mofka::DataDescriptor::Segment{s.first, s.second});
                return data_descriptor.makeUnstructuredView(seg);
            }, R"(
            Construct a new DataDescriptor by taking a selection of segments from the current DataDescriptor.

            Parameters
            ----------

            segments (list[tuple[int,int]]): list of segments.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "segments"_a)
    ;

    py::class_<mofka::EventInterface,
               std::shared_ptr<mofka::EventInterface>>(m, "Event")
        .def_property_readonly("metadata",
                [](mofka::EventInterface& event) { return event.metadata().json(); },
                "Metadata of the event.")
        .def_property_readonly("data",
                [](mofka::EventInterface& event) {
                    auto owner = static_cast<AbstractDataOwner*>(event.data().context());
                    return owner->toPythonObject();
                },
                "Data attached to the event.")
        .def_property_readonly("event_id", [](const mofka::EventInterface& event) -> py::object {
                if(event.id() == mofka::NoMoreEvents)
                    return py::none();
                else
                    return py::cast(event.id());
                }, "Event ID in its partition.")
        .def_property_readonly("partition",
                [](const mofka::EventInterface& event) {
                    return event.partition().json();
                }, "Backend-specific information on the partition this event comes from.")
        .def("acknowledge",
             [](const mofka::EventInterface& event){
                event.acknowledge();
             }, "Acknowledge the event so it is not re-consumed if the consumer restarts.")
    ;

    py::class_<mofka::Future<std::uint64_t>,
               std::shared_ptr<mofka::Future<std::uint64_t>>>(m, "FutureUint")
        .def("wait", [](mofka::Future<std::uint64_t>& future) {
            std::uint64_t result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        }, "Wait for the future to complete, returning its value (int) when it does.")
        .def_property_readonly(
            "completed",
            &mofka::Future<std::uint64_t>::completed,
            "Checks whether the future has completed.")
    ;

    py::class_<mofka::Future<mofka::Event>,
               std::shared_ptr<mofka::Future<mofka::Event>>>(m, "FutureEvent")
        .def("wait", [](mofka::Future<mofka::Event>& future) {
                mofka::Event result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        }, "Wait for the future to complete, returning its value (Event) when it does.")
        .def("completed", &mofka::Future<mofka::Event>::completed,
             "Checks whether the future has completed.")
    ;

    PythonDataSelector select_full_data =
        [](const nlohmann::json&, const mofka::DataDescriptor& d) -> std::optional<mofka::DataDescriptor> {
            return d;
        };
    m.attr("FullDataSelector") = py::cast(select_full_data);

    PythonDataBroker bytes_data_broker =
        [](const nlohmann::json&, const mofka::DataDescriptor& d) -> py::list {
            auto buffer = py::bytearray();
            auto ret = PyByteArray_Resize(buffer.ptr(), d.size());
            py::list result;
            result.append(std::move(buffer));
            return result;
        };
    m.attr("ByteArrayAllocator") = py::cast(bytes_data_broker);
}
