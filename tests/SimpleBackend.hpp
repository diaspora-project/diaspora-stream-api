#include <mofka/Consumer.hpp>
#include <mofka/Producer.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/Driver.hpp>
#include <mofka/BufferWrapperArchive.hpp>

#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <variant>


class SimpleDriver;
class SimpleProducer;
class SimpleConsumer;
class SimpleTopicHandle;


template<typename T>
struct FutureState {

    std::mutex                        mutex;
    std::condition_variable           cv;
    std::variant<T, mofka::Exception> value;
    bool                              is_set = false;

    template<typename U>
    void set(U u) {
        if(is_set) throw mofka::Exception{"Promise already set"};
        {
            std::unique_lock lock{mutex};
            value = std::move(u);
            is_set = true;
        }
        cv.notify_all();
    }

    T wait() {
        std::unique_lock lock{mutex};
        while(!is_set) {
            cv.wait(lock);
        }
        if(std::holds_alternative<T>(value))
            return std::get<T>(value);
        else
            throw std::get<mofka::Exception>(value);
    }

    bool test() {
        std::unique_lock lock{mutex};
        return is_set;
    }
};


class SimpleThreadPool final : public mofka::ThreadPoolInterface {

    struct Work {

        uint64_t              priority;
        std::function<void()> func;

        Work(uint64_t p, std::function<void()> f)
        : priority(p)
        , func{std::move(f)} {}

        friend bool operator<(const Work& lhs, const Work& rhs) {
            return lhs.priority < rhs.priority;
        }
    };

    std::vector<std::thread>  m_threads;
    std::priority_queue<Work> m_queue;
    mutable std::mutex        m_queue_mtx;
    std::condition_variable   m_queue_cv;
    std::atomic<bool>         m_must_stop = false;

    public:

    SimpleThreadPool(mofka::ThreadCount count) {
        m_threads.reserve(count.count);
        for(size_t i = 0; i < count.count; ++i) {
            m_threads.emplace_back([this]() {
                while(!m_must_stop) {
                    std::unique_lock<std::mutex> lock{m_queue_mtx};
                    if(m_queue.empty()) m_queue_cv.wait(lock);
                    if(m_queue.empty()) continue;
                    auto work = m_queue.top();
                    m_queue.pop();
                    lock.unlock();
                    work.func();
                }
            });
        }
    }

    ~SimpleThreadPool() {
        m_must_stop = true;
        m_queue_cv.notify_all();
        for(auto& th : m_threads) th.join();
    }

    mofka::ThreadCount threadCount() const override {
        return mofka::ThreadCount{m_threads.size()};
    }

    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) override {
        if(m_threads.size() == 0) {
            func();
        } else {
            {
                std::unique_lock<std::mutex> lock{m_queue_mtx};
                m_queue.emplace(priority, std::move(func));
            }
            m_queue_cv.notify_one();
        }
    }

    size_t size() const override {
        std::unique_lock<std::mutex> lock{m_queue_mtx};
        return m_queue.size();
    }
};


class SimpleEvent : public mofka::EventInterface {

    mofka::Metadata      m_metadata;
    mofka::DataView      m_data;
    mofka::PartitionInfo m_partition;
    mofka::EventID       m_id;

    public:

    SimpleEvent(mofka::Metadata metadata,
                mofka::DataView data,
                mofka::PartitionInfo partition,
                mofka::EventID id)
    : m_metadata(std::move(metadata))
    , m_data(std::move(data))
    , m_partition(std::move(partition))
    , m_id(id) {}

    mofka::Metadata metadata() const override {
        return m_metadata;
    }

    mofka::DataView data() const override {
        return m_data;
    }

    mofka::PartitionInfo partition() const override {
        return m_partition;
    }

    mofka::EventID id() const override {
        return m_id;
    }

    void acknowledge() const override {}

};


class SimpleProducer final : public mofka::ProducerInterface {

    const std::string                        m_name;
    const mofka::BatchSize                   m_batch_size;
    const mofka::MaxNumBatches               m_max_num_batches;
    const mofka::Ordering                    m_ordering;
    const std::shared_ptr<SimpleThreadPool>  m_thread_pool;
    const std::shared_ptr<SimpleTopicHandle> m_topic;

    public:

    SimpleProducer(
        std::string name,
        mofka::BatchSize batch_size,
        mofka::MaxNumBatches max_num_batches,
        mofka::Ordering ordering,
        std::shared_ptr<SimpleThreadPool> thread_pool,
        std::shared_ptr<SimpleTopicHandle> topic)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_ordering(ordering)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic)) {}

    const std::string& name() const override {
        return m_name;
    }

    mofka::BatchSize batchSize() const override {
        return m_batch_size;
    }

    mofka::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    mofka::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<mofka::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<mofka::TopicHandleInterface> topic() const override;

    mofka::Future<mofka::EventID> push(
            mofka::Metadata metadata,
            mofka::DataView data,
            std::optional<size_t> partition) override;

    void flush() override {}
};


class SimpleConsumer final : public mofka::ConsumerInterface {

    const std::string                        m_name;
    const mofka::BatchSize                   m_batch_size;
    const mofka::MaxNumBatches               m_max_num_batches;
    const std::shared_ptr<SimpleThreadPool>  m_thread_pool;
    const std::shared_ptr<SimpleTopicHandle> m_topic;
    const mofka::DataBroker                  m_data_broker;
    const mofka::DataSelector                m_data_selector;

    size_t                                   m_next_offset = 0;

    public:

    SimpleConsumer(
        std::string name,
        mofka::BatchSize batch_size,
        mofka::MaxNumBatches max_num_batches,
        std::shared_ptr<SimpleThreadPool> thread_pool,
        std::shared_ptr<SimpleTopicHandle> topic,
        mofka::DataBroker data_broker,
        mofka::DataSelector data_selector)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic))
    , m_data_broker{std::move(data_broker)}
    , m_data_selector{std::move(data_selector)}
    {}

    const std::string& name() const override {
        return m_name;
    }

    mofka::BatchSize batchSize() const override {
        return m_batch_size;
    }

    mofka::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    std::shared_ptr<mofka::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<mofka::TopicHandleInterface> topic() const override;

    const mofka::DataBroker& dataBroker() const override {
        return m_data_broker;
    }

    const mofka::DataSelector& dataSelector() const override {
        return m_data_selector;
    }

    void process(mofka::EventProcessor processor,
                 std::shared_ptr<mofka::ThreadPoolInterface> threadPool,
                 mofka::NumEvents maxEvents) override;

    void unsubscribe() override;

    mofka::Future<mofka::Event> pull() override;

};


class SimpleTopicHandle final : public mofka::TopicHandleInterface,
                                public std::enable_shared_from_this<SimpleTopicHandle> {

    friend class SimpleProducer;
    friend class SimpleConsumer;

    struct Partition {
        std::vector<std::vector<char>> metadata;
        std::vector<std::vector<char>> data;
    };

    const std::string                       m_name;
    const std::vector<mofka::PartitionInfo> m_pinfo = {mofka::PartitionInfo("{}")};
    const mofka::Validator                  m_validator;
    const mofka::PartitionSelector          m_partition_selector;
    const mofka::Serializer                 m_serializer;
    const std::shared_ptr<SimpleDriver>     m_driver;

    Partition                               m_partition;
    bool                                    m_is_complete = false;

    public:

    SimpleTopicHandle(
        std::string name,
        mofka::Validator validator,
        mofka::PartitionSelector partition_selector,
        mofka::Serializer serializer,
        std::shared_ptr<SimpleDriver> driver)
    : m_name{std::move(name)}
    , m_validator(std::move(validator))
    , m_partition_selector(std::move(partition_selector))
    , m_serializer(std::move(serializer))
    , m_driver{std::move(driver)}
    {}

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<mofka::DriverInterface> driver() const override;

    const std::vector<mofka::PartitionInfo>& partitions() const override {
        return m_pinfo;
    }

    mofka::Validator validator() const override {
        return m_validator;
    }

    mofka::PartitionSelector selector() const override {
        return m_partition_selector;
    }

    mofka::Serializer serializer() const override {
        return m_serializer;
    }

    void markAsComplete() override {
        m_is_complete = true;
    }

    std::shared_ptr<mofka::ProducerInterface>
        makeProducer(std::string_view name,
                     mofka::BatchSize batch_size,
                     mofka::MaxNumBatches max_batch,
                     mofka::Ordering ordering,
                     std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
                     mofka::Metadata options) override;

    std::shared_ptr<mofka::ConsumerInterface>
        makeConsumer(std::string_view name,
                     mofka::BatchSize batch_size,
                     mofka::MaxNumBatches max_batch,
                     std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
                     mofka::DataBroker data_broker,
                     mofka::DataSelector data_selector,
                     const std::vector<size_t>& targets,
                     mofka::Metadata options) override;
};

inline std::shared_ptr<mofka::TopicHandleInterface> SimpleProducer::topic() const {
    return m_topic;
}

inline std::shared_ptr<mofka::TopicHandleInterface> SimpleConsumer::topic() const {
    return m_topic;
}

inline mofka::Future<mofka::EventID> SimpleProducer::push(
        mofka::Metadata metadata,
        mofka::DataView data,
        std::optional<size_t> partition) {
    auto state = std::make_shared<FutureState<mofka::EventID>>();
    m_thread_pool->pushWork(
        [topic=m_topic, state,
         metadata=std::move(metadata),
         data=std::move(data),
         partition]() {
            try {
                // validation
                topic->validator().validate(metadata, data);
                // serialization
                std::vector<char> metadata_buffer, data_buffer;
                mofka::BufferWrapperOutputArchive archive(metadata_buffer);
                topic->serializer().serialize(archive, metadata);
                data_buffer.resize(data.size());
                data.read(data_buffer.data(), data_buffer.size());
                // partition selection
                auto index = topic->m_partition_selector.selectPartitionFor(metadata, partition);
                if(index != 0) throw mofka::Exception{"Invalid index returned by PartitionSelector"};
                auto& metadata_vector = topic->m_partition.metadata;
                auto& data_vector = topic->m_partition.data;
                metadata_vector.push_back(std::move(metadata_buffer));
                data_vector.push_back(std::move(data_buffer));
                auto event_id = metadata_vector.size()-1;
                // set the ID
                state->set(event_id);
            } catch(const mofka::Exception& ex) {
                state->set(ex);
            }
        });
    return mofka::Future<mofka::EventID>{
        [state] { return state->wait(); },
        [state] { return state->test(); }
    };
}

class SimpleDriver : public mofka::DriverInterface,
                     public std::enable_shared_from_this<SimpleDriver> {

    std::shared_ptr<mofka::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<SimpleThreadPool>(mofka::ThreadCount{0});
    std::unordered_map<std::string, std::shared_ptr<SimpleTopicHandle>> m_topics;

    public:

    void createTopic(std::string_view name,
                     mofka::Metadata options,
                     mofka::Validator validator,
                     mofka::PartitionSelector selector,
                     mofka::Serializer serializer) override {
        (void)options;
        if(m_topics.count(std::string{name})) throw mofka::Exception{"Topic already exists"};
        std::vector<mofka::PartitionInfo> pinfo{mofka::PartitionInfo{"{}"}};
        selector.setPartitions(pinfo);
        m_topics.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::string{name}),
            std::forward_as_tuple(
                std::make_shared<SimpleTopicHandle>(
                    std::string{name},
                    std::move(validator),
                    std::move(selector),
                    std::move(serializer),
                    shared_from_this()
                )
            )
        );
    }

    std::shared_ptr<mofka::TopicHandleInterface> openTopic(std::string_view name) const override {
        auto it = m_topics.find(std::string{name});
        if(it == m_topics.end()) return nullptr;
        else return it->second;
    }

    bool topicExists(std::string_view name) const override {
        return m_topics.count(std::string{name});
    }

    std::shared_ptr<mofka::ThreadPoolInterface> defaultThreadPool() const override {
        return m_default_thread_pool;
    }

    std::shared_ptr<mofka::ThreadPoolInterface> makeThreadPool(mofka::ThreadCount count) const override {
        return std::make_shared<SimpleThreadPool>(count);
    }

    static inline std::shared_ptr<mofka::DriverInterface> create(const mofka::Metadata&) {
        return std::make_shared<SimpleDriver>();
    }
};


inline std::shared_ptr<mofka::DriverInterface> SimpleTopicHandle::driver() const {
    return m_driver;
}

inline std::shared_ptr<mofka::ProducerInterface>
    SimpleTopicHandle::makeProducer(std::string_view name,
        mofka::BatchSize batch_size,
        mofka::MaxNumBatches max_batch,
        mofka::Ordering ordering,
        std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
        mofka::Metadata options) {
    (void)options;
    auto simple_thread_pool = std::dynamic_pointer_cast<SimpleThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw mofka::Exception{"ThreadPool should be an instance of SimpleThreadPool"};
    return std::make_shared<SimpleProducer>(
            std::string{name}, batch_size, max_batch, ordering, simple_thread_pool,
            shared_from_this());
}

inline std::shared_ptr<mofka::ConsumerInterface>
    SimpleTopicHandle::makeConsumer(std::string_view name,
        mofka::BatchSize batch_size,
        mofka::MaxNumBatches max_batch,
        std::shared_ptr<mofka::ThreadPoolInterface> thread_pool,
        mofka::DataBroker data_broker,
        mofka::DataSelector data_selector,
        const std::vector<size_t>& targets,
        mofka::Metadata options) {
    (void)options;
    (void)targets;
    auto simple_thread_pool = std::dynamic_pointer_cast<SimpleThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw mofka::Exception{"ThreadPool should be an instance of SimpleThreadPool"};
    return std::make_shared<SimpleConsumer>(
            std::string{name}, batch_size, max_batch, simple_thread_pool,
            shared_from_this(), std::move(data_broker),
            std::move(data_selector));
}

void SimpleConsumer::unsubscribe() {}

mofka::Future<mofka::Event> SimpleConsumer::pull() {
    if(m_next_offset == m_topic->m_partition.metadata.size()) {
        return mofka::Future<mofka::Event>{
            []() { return mofka::Event(std::make_shared<SimpleEvent>(
                        mofka::Metadata{}, mofka::DataView{}, mofka::PartitionInfo{},
                        mofka::NoMoreEvents));},
            []() { return true; }
        };
    } else {
        // get the metadata and data from the topic
        auto& metadata_buffer = m_topic->m_partition.metadata[m_next_offset];
        auto& data_buffer     = m_topic->m_partition.data[m_next_offset];
        mofka::Metadata metadata;
        mofka::DataView data;
        // deserialize metadata
        mofka::BufferWrapperInputArchive archive(
            std::string_view{metadata_buffer.data(), metadata_buffer.size()});
        m_topic->m_serializer.deserialize(archive, metadata);
        // invoke the data selector
        auto data_descriptor = m_data_selector(
            metadata, mofka::DataDescriptor::From("", data_buffer.size()));
        // invoke the data broker
        auto data_view = m_data_broker(metadata, data_descriptor);
        // copy the data to target destination
        data_view.write(data_buffer.data(), data_buffer.size());
        m_next_offset += 1;
        return mofka::Future<mofka::Event>{
            [metadata=std::move(metadata), data=std::move(data), event_id=m_next_offset-1]() {
                return mofka::Event(std::make_shared<SimpleEvent>(
                        std::move(metadata), std::move(data), mofka::PartitionInfo{},
                        event_id));},
            []() { return true; }
        };
    }
}

inline void SimpleConsumer::process(
        mofka::EventProcessor processor,
        std::shared_ptr<mofka::ThreadPoolInterface> threadPool,
        mofka::NumEvents maxEvents) {
    if(!threadPool) threadPool = m_topic->driver()->defaultThreadPool();
    size_t                  pending_events = 0;
    std::mutex              pending_mutex;
    std::condition_variable pending_cv;
    try {
        for(size_t i = 0; i < maxEvents.value; ++i) {
            auto event = pull().wait();
            {
                std::unique_lock lock{pending_mutex};
                pending_events += 1;
            }
            threadPool->pushWork([&, event=std::move(event)]() {
                processor(event);
                std::unique_lock lock{pending_mutex};
                pending_events -= 1;
                if(pending_events == 0)
                    pending_cv.notify_all();
            });
        }
    } catch(const mofka::StopEventProcessor&) {}
    std::unique_lock lock{pending_mutex};
    while(pending_events) pending_cv.wait(lock);
}
