/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_CONSUMER_HPP
#define MOFKA_API_CONSUMER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataView.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Future.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/DataBroker.hpp>
#include <mofka/DataSelector.hpp>
#include <mofka/EventProcessor.hpp>
#include <mofka/BatchParams.hpp>
#include <mofka/NumEvents.hpp>
#include <memory>

namespace mofka {

/**
 * @brief Interface for Consumer class.
 */
class ConsumerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ConsumerInterface() = default;

    /**
     * @brief Returns the name of the producer.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Returns a copy of the options provided when
     * the Consumer was created.
     */
    virtual BatchSize batchSize() const = 0;

    /**
     * @brief Returns the maximum number of batches the
     * Consumer is allowed to hold at any time.
     */
    virtual MaxNumBatches maxNumBatches() const = 0;

    /**
     * @brief Returns the ThreadPool associated with the Consumer.
     */
    virtual std::shared_ptr<ThreadPoolInterface> threadPool() const = 0;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    virtual std::shared_ptr<TopicHandleInterface> topic() const = 0;

    /**
     * @brief Returns the DataBroker used by the Consumer.
     */
    virtual const DataBroker& dataBroker() const = 0;

    /**
     * @brief Returns the DataSelector used by the Consumer.
     */
    virtual const DataSelector& dataSelector() const = 0;

    /**
     * @brief Feed the Events pulled by the Consumer into the provided
     * EventProcessor function. The Consumer will stop feeding the processor
     * if it raises a StopEventProcessor exception, or after maxEvents events
     * have been processed.
     *
     * @param processor EventProcessor.
     * @param threadPool ThreadPool in which to submit processing jobs.
     * @param maxEvents Maximum number of events to process.
     */
    virtual void process(EventProcessor processor,
                         std::shared_ptr<ThreadPoolInterface> threadPool,
                         NumEvents maxEvents) = 0;

    /**
     * @brief Unsubscribe from the topic.
     *
     * This function is not supposed to be called by users directly.
     * It is used by the Consumer wrapping the ConsumerInterface in
     * its destructor to stop events from coming in before destroying
     * the object itself.
     */
    virtual void unsubscribe() = 0;

    /**
     * @brief Pull an Event. This function will immediately
     * return a Future<Event>. Calling wait() on the event will
     * block until an Event is actually available.
     */
    virtual Future<Event> pull() = 0;
};

/**
 * @brief A Consumer is an object that can emmit events into a its topic.
 * The Consumer class is a convenient wrapper around the ConsumerInterface
 * to provide a pimpl design.
 */
class Consumer {

    friend class TopicHandle;

    public:

    inline Consumer() = default;

    /**
     * @brief Copy-constructor.
     */
    inline Consumer(const Consumer&) = default;

    /**
     * @brief Move-constructor.
     */
    inline Consumer(Consumer&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    inline Consumer& operator=(const Consumer&) = default;

    /**
     * @brief Move-assignment operator.
     */
    inline Consumer& operator=(Consumer&&) = default;

    /**
     * @brief Destructor.
     */
    ~Consumer();

    /**
     * @brief Returns the name of the producer.
     */
    inline const std::string& name() const {
        return self->name();
    }

    /**
     * @brief Returns a copy of the options provided when
     * the Consumer was created.
     */
    inline BatchSize batchSize() const {
        return self->batchSize();
    }

    /**
     * @brief Returns the maximum number of batches the
     * Consumer is allowed to hold at any time.
     */
    inline MaxNumBatches maxNumBatch() const {
        return self->maxNumBatches();
    }

    /**
     * @brief Returns the ThreadPool associated with the Consumer.
     */
    inline ThreadPool threadPool() const {
        return self->threadPool();
    }

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Returns the DataBroker used by the Consumer.
     */
    inline decltype(auto) dataBroker() const {
        return self->dataBroker();
    }

    /**
     * @brief Returns the DataSelector used by the Consumer.
     */
    inline decltype(auto) dataSelector() const {
        return self->dataSelector();
    }

    /**
     * @brief Pull an Event. This function will immediately
     * return a Future<Event>. Calling wait() on the event will
     * block until an Event is actually available.
     */
    inline Future<Event> pull() const {
        return self->pull();
    }

    /**
     * @brief Feed the Events pulled by the Consumer into the provided
     * EventProcessor function. The Consumer will stop feeding the processor
     * if it raises a StopEventProcessor exception, or after maxEvents events
     * have been processed.
     *
     * @note Calling process from multiple threads concurrently on the same
     * consumer is not allowed and will throw an exception.
     *
     * @param processor EventProcessor.
     */
    void process(EventProcessor processor,
                 ThreadPool threadPool = ThreadPool{},
                 NumEvents maxEvents = NumEvents::Infinity()) const {
        self->process(std::move(processor),
                      threadPool.self,
                      maxEvents);
    }

    /**
     * @brief This method is syntactic sugar to call process with
     * the threadPool set to the same ThreadPool as the Consumer
     * and a maxEvents set to infinity.
     *
     * Note: this method can only be called on a rvalue reference to a
     * Consumer, e.g. doing:
     * ```
     * topic.consumer("myconsumer", ...) | processor;
     * ```
     * This is to prevent another thread from calling it with another
     * processor.
     *
     * @param processor EventProcessor.
     */
    inline void operator|(EventProcessor processor) const && {
        process(processor, threadPool(), NumEvents::Infinity());
    }

    /**
     * @brief Checks if the Consumer instance is valid.
     */
    inline explicit operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    inline Consumer(const std::shared_ptr<ConsumerInterface>& impl)
    : self{impl} {}

    std::shared_ptr<ConsumerInterface> self;
};

}

#endif
