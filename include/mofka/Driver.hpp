/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_DRIVER_HPP
#define MOFKA_API_DRIVER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Serializer.hpp>
#include <mofka/Validator.hpp>
#include <mofka/PartitionSelector.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/TopicHandle.hpp>

#include <memory>

namespace mofka {

/**
 * @brief A DriverInterface object is a handle for a Mofka service
 * deployed on a set of servers.
 */
class DriverInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~DriverInterface() = default; // LCOV_EXCL_LINE

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param options Options (e.g. number of partitions, replication, etc.)
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    virtual void createTopic(std::string_view name,
                             const Metadata& options,
                             std::shared_ptr<ValidatorInterface> validator,
                             std::shared_ptr<PartitionSelectorInterface> selector,
                             std::shared_ptr<SerializerInterface> serializer) = 0;

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    virtual std::shared_ptr<TopicHandleInterface> openTopic(std::string_view name) const = 0;

    /**
     * @brief Checks if a topic exists.
     */
    virtual bool topicExists(std::string_view name) const = 0;

    /**
     * @brief Get the default ThreadPool.
     */
    virtual std::shared_ptr<ThreadPoolInterface> defaultThreadPool() const = 0;

    /**
     * @brief Create a ThreadPool.
     *
     * @param count Number of threads.
     *
     * @return a ThreadPool with the specified number of threads.
     */
    virtual std::shared_ptr<ThreadPoolInterface> makeThreadPool(ThreadCount count) const = 0;

    /**
     * @note A DriverInterface class must also provide a static Create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<DriverInterface> create(const Metadata&);
     */

};

using DriverFactory = Factory<DriverInterface, const Metadata&>;

class Driver {

    friend class TopicHandle;

    public:

    Driver() = default; // LCOV_EXCL_LINE
    Driver(const Driver&) = default; // LCOV_EXCL_LINE
    Driver(Driver&&) = default; // LCOV_EXCL_LINE
    Driver& operator=(const Driver&) = default; // LCOV_EXCL_LINE
    Driver& operator=(Driver&&) = default; // LCOV_EXCL_LINE
    ~Driver() = default; // LCOV_EXCL_LINE

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    inline void createTopic(std::string_view name,
                            const Metadata& options = Metadata{"{}"},
                            Validator validator = Validator{},
                            PartitionSelector selector = PartitionSelector{},
                            Serializer serializer = Serializer{}) const {
        self->createTopic(name, std::move(options),
                          std::move(validator).self, std::move(selector).self,
                          std::move(serializer).self);
    }

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    inline TopicHandle openTopic(std::string_view name) const {
        auto topic = self->openTopic(name);
        if(!topic) throw Exception{"Topic " + std::string{name} + " does not exist"};
        return TopicHandle{self->openTopic(name)};
    }

    /**
     * @brief Checks if a topic exists.
     */
    inline bool topicExists(std::string_view name) const {
        return self->topicExists(name);
    }

    /**
     * @brief Get the default ThreadPool.
     */
    inline ThreadPool defaultThreadPool() const {
        return ThreadPool{self->defaultThreadPool()};
    }

    /**
     * @brief Create a ThreadPool.
     *
     * @param count Number of threads.
     *
     * @return a ThreadPool with the specified number of threads.
     */
    inline ThreadPool makeThreadPool(ThreadCount count) const {
        return ThreadPool{self->makeThreadPool(count)};
    }

    /**
     * @brief Checks if the Driver instance is valid.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(self);
    }

    static inline Driver New(const char* type, const Metadata& options) {
        return DriverFactory::create(type, options);
    }

    /**
     * @brief Try to convert into a reference to the underlying type.
     */
    template<typename T>
    T& as() {
        T* ptr = std::dynamic_pointer_cast<T>(self);
        if(ptr) return *ptr;
        else throw Exception{"Invalid type convertion requested"};
    }

    private:

    Driver(const std::shared_ptr<DriverInterface>& impl)
    : self{impl} {}

    std::shared_ptr<DriverInterface> self;
};

#define MOFKA_REGISTER_DRIVER(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(DriverFactory, __type__, __name__)

}

#endif
