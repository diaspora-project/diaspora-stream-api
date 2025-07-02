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
#include "../src/DataViewImpl.hpp"
#include "../src/JsonUtil.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")

namespace mofka {

struct PythonBindingHelper {
    template<typename T>
    static auto GetSelf(const T& x) {
        return x.self;
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
    auto free_cb = [owner](mofka::DataView::Context) { delete owner; };
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
    auto free_cb = [owner](mofka::DataView::Context) { delete owner; };
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

PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::register_exception<mofka::Exception>(m, "Exception", PyExc_RuntimeError);

    m.attr("AdaptiveBatchSize") = py::int_(mofka::BatchSize::Adaptive().value);

    py::class_<mofka::ValidatorInterface,
               std::shared_ptr<mofka::ValidatorInterface>>(m, "Validator")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PythonBindingHelper::GetSelf(
                    mofka::Validator::FromMetadata(md));
            }, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::ThreadPoolInterface,
               std::shared_ptr<mofka::ThreadPoolInterface>>(m, "ThreadPool")
        .def_property_readonly("thread_count",
            [](const mofka::ThreadPoolInterface& thread_pool) -> std::size_t {
                return thread_pool.threadCount().count;
            })
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
            }, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::PartitionSelectorInterface,
               std::shared_ptr<mofka::PartitionSelectorInterface>>(m, "PartitionSelector")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PythonBindingHelper::GetSelf(
                    mofka::PartitionSelector::FromMetadata(md));
            }, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::TopicHandleInterface,
               std::shared_ptr<mofka::TopicHandleInterface>>(m, "TopicHandle")
        .def_property_readonly("name", &mofka::TopicHandleInterface::name)
        .def_property_readonly("partitions", [](const mofka::TopicHandle& topic) {
            std::vector<nlohmann::json> result;
            for(auto& p : topic.partitions()) {
                result.push_back(p.json());
            }
            return result;
        })
        .def("mark_as_complete", &mofka::TopicHandleInterface::markAsComplete)
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
                        auto free_cb = [owner](mofka::DataView::Context) { delete owner; };
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
                        else return mofka::DataDescriptor::Null();
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
            "name"_a, py::kw_only(),
            "data_selector"_a, "data_broker"_a,
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_num_batches"_a=2, "thread_pool"_a=mofka::ThreadPool{},
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
                        auto free_cb = [owner](mofka::DataView::Context) { delete owner; };
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
            "name"_a, py::kw_only(),
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_num_batches"_a=2, "thread_pool"_a=mofka::ThreadPool{},
            "targets"_a=std::optional<std::vector<size_t>>{},
            "options"_a=nlohmann::json(nullptr))
    ;

    py::class_<mofka::ProducerInterface,
               std::shared_ptr<mofka::ProducerInterface>>(m, "Producer")
        .def_property_readonly("name", &mofka::ProducerInterface::name)
        .def_property_readonly("thread_pool", &mofka::ProducerInterface::threadPool)
        .def_property_readonly("topic", &mofka::ProducerInterface::topic)
        .def("push",
            [](mofka::ProducerInterface& producer,
               std::string metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               nlohmann::json metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               std::string metadata,
               const py::list& b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=std::vector<py::memoryview>{},
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](mofka::ProducerInterface& producer,
               nlohmann::json metadata,
               py::list b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("flush", &mofka::ProducerInterface::flush)
        .def("batch_size",
            [](const mofka::ProducerInterface& producer) -> std::size_t {
                return producer.batchSize().value;
            })
    ;

    py::class_<mofka::ConsumerInterface,
               std::shared_ptr<mofka::ConsumerInterface>>(m, "Consumer")
        .def_property_readonly("name", &mofka::ConsumerInterface::name)
        .def_property_readonly("thread_pool", &mofka::ConsumerInterface::threadPool)
        .def_property_readonly("topic", &mofka::ConsumerInterface::topic)
        .def_property_readonly("data_broker", &mofka::ConsumerInterface::dataBroker)
        .def_property_readonly("data_selector", &mofka::ConsumerInterface::dataSelector)
        .def("batch_size",
            [](const mofka::ConsumerInterface& consumer) -> std::size_t {
                return consumer.batchSize().value;
            })
        .def("pull", &mofka::ConsumerInterface::pull)
        .def("process",
            [](mofka::ConsumerInterface& consumer,
               mofka::EventProcessor processor,
               std::shared_ptr<mofka::ThreadPoolInterface> threadPool,
               std::size_t maxEvents) {
                return consumer.process(processor, threadPool, mofka::NumEvents{maxEvents});
               },
            "processor"_a, py::kw_only(), "thread_pool"_a,
            "max_events"_a=std::numeric_limits<size_t>::max()
            )
    ;

    py::class_<mofka::DataDescriptor>(m, "DataDescriptor")
        .def(py::init<>())
        .def(py::init(&mofka::DataDescriptor::From))
        .def_property_readonly("size", &mofka::DataDescriptor::size)
        .def_property_readonly("location",
            py::overload_cast<>(&mofka::DataDescriptor::location))
        .def_property_readonly("location",
            py::overload_cast<>(&mofka::DataDescriptor::location, py::const_))
        .def("make_stride_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t numblocks,
               std::size_t blocksize,
               std::size_t gapsize) -> mofka::DataDescriptor {
                return data_descriptor.makeStridedView(offset, numblocks, blocksize, gapsize);
            },
            "offset"_a, "numblocks"_a, "blocksize"_a, "gapsize"_a)
        .def("make_sub_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t size) -> mofka::DataDescriptor {
                return data_descriptor.makeSubView(offset, size);
            },
            "offset"_a, "size"_a)
        .def("make_unstructured_view",
            [](const mofka::DataDescriptor& data_descriptor,
               const std::vector<std::pair<std::size_t, std::size_t>> segments) -> mofka::DataDescriptor {
                return data_descriptor.makeUnstructuredView(segments);
            },
            "segments"_a)
    ;

    py::class_<mofka::EventInterface,
               std::shared_ptr<mofka::EventInterface>>(m, "Event")
        .def_property_readonly("metadata",
                [](mofka::EventInterface& event) { return event.metadata().json(); })
        .def_property_readonly("data",
                [](mofka::EventInterface& event) {
                    auto owner = static_cast<AbstractDataOwner*>(event.data().context());
                    return owner->toPythonObject();
                })
        .def_property_readonly("event_id", [](const mofka::EventInterface& event) -> py::object {
                if(event.id() == mofka::NoMoreEvents)
                    return py::none();
                else
                    return py::cast(event.id());
                })
        .def_property_readonly("partition",
                [](const mofka::EventInterface& event) {
                    return event.partition().json();
                })
        .def("acknowledge",
             [](const mofka::EventInterface& event){
                event.acknowledge();
             })
    ;

    py::class_<mofka::Future<std::uint64_t>,
               std::shared_ptr<mofka::Future<std::uint64_t>>>(m, "FutureUint")
        .def("wait", [](mofka::Future<std::uint64_t>& future) {
            std::uint64_t result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        })
        .def_property_readonly("completed", &mofka::Future<std::uint64_t>::completed)
    ;

    py::class_<mofka::Future<mofka::Event>,
               std::shared_ptr<mofka::Future<mofka::Event>>>(m, "FutureEvent")
        .def("wait", [](mofka::Future<mofka::Event>& future) {
                mofka::Event result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        })
        .def("completed", &mofka::Future<mofka::Event>::completed)
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
