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
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    virtual void createTopic(std::string_view name,
                             Validator validator = Validator{},
                             PartitionSelector selector = PartitionSelector{},
                             Serializer serializer = Serializer{}) const = 0;

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    virtual TopicHandle openTopic(std::string_view name) const = 0;

    /**
     * @brief Checks if a topic exists.
     */
    virtual bool topicExists(std::string_view name) const = 0;

    /**
     * @brief Get the default ThreadPool.
     */
    virtual ThreadPool defaultThreadPool() const = 0;

    /**
     * @brief Create a ThreadPool.
     *
     * @param count Number of threads.
     *
     * @return a ThreadPool with the specified number of threads.
     */
    virtual ThreadPool makeThreadPool(ThreadCount count) const = 0;

};

class Driver {

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
                     Validator validator = Validator{},
                     PartitionSelector selector = PartitionSelector{},
                     Serializer serializer = Serializer{}) const {
        self->createTopic(name, validator, selector, serializer);
    }

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    inline TopicHandle openTopic(std::string_view name) const {
        return self->openTopic(name);
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
        return self->defaultThreadPool();
    }

    /**
     * @brief Create a ThreadPool.
     *
     * @param count Number of threads.
     *
     * @return a ThreadPool with the specified number of threads.
     */
    inline ThreadPool makeThreadPool(ThreadCount count) const {
        return self->makeThreadPool(count);
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
