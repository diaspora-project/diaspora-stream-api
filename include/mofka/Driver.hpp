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
    virtual ~DriverInterface() = default;

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
                             Metadata options,
                             Validator validator,
                             PartitionSelector selector,
                             Serializer serializer) = 0;

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

};

class Driver {

    friend class TopicHandle;

    public:

    Driver() = default;
    Driver(const Driver&) = default;
    Driver(Driver&&) = default;
    Driver& operator=(const Driver&) = default;
    Driver& operator=(Driver&&) = default;
    ~Driver() = default;

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    inline void createTopic(std::string_view name,
                            Metadata options = Metadata{"{}"},
                            Validator validator = Validator{},
                            PartitionSelector selector = PartitionSelector{},
                            Serializer serializer = Serializer{}) const {
        self->createTopic(name, std::move(options),
                          std::move(validator), std::move(selector),
                          std::move(serializer));
    }

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    inline TopicHandle openTopic(std::string_view name) const {
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

    private:

    Driver(const std::shared_ptr<DriverInterface>& impl)
    : self{impl} {}

    std::shared_ptr<DriverInterface> self;
};

using DriverFactory = Factory<DriverInterface, const Metadata&>;

#define MOFKA_REGISTER_DRIVER(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(DriverFactory, __type__, __name__)

}

#endif
