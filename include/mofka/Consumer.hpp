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
#include <mofka/DataAllocator.hpp>
#include <mofka/DataSelector.hpp>
#include <mofka/EventProcessor.hpp>
#include <mofka/BatchParams.hpp>
#include <mofka/NumEvents.hpp>
#include <memory>

namespace mofka {

/**
 * @brief Interface for Consumer class.
 */
class ConsumerInterface : public std::enable_shared_from_this<ConsumerInterface> {

    public:

    class Iterator {

        friend class ConsumerInterface;

        public:

        using iterator_category = std::forward_iterator_tag;
        using value_type = std::shared_ptr<EventInterface>;
        using difference_type = std::ptrdiff_t;
        using pointer = std::shared_ptr<EventInterface>;
        using reference = EventInterface&;

        inline Iterator() = default;

        inline reference operator*() const { return *m_current_event; }

        inline pointer operator->() const { return m_current_event; }

        inline Iterator& operator++() {
            m_current_event = static_cast<std::shared_ptr<EventInterface>>(m_owner->pull().wait());
            if(m_current_event->id() == NoMoreEvents) {
                m_owner = nullptr;
                m_current_event = nullptr;
            }
            return *this;
        }

        inline bool operator==(const Iterator& other) const {
            if(!other.m_owner && !m_owner) {
                return true;
            }
            return false;
        }

        inline bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

        private:

        Iterator(std::shared_ptr<ConsumerInterface> owner)
        : m_owner(std::move(owner)) {
            ++(*this);
        }

        std::shared_ptr<EventInterface>    m_current_event;
        std::shared_ptr<ConsumerInterface> m_owner;
    };

    /**
     * @brief Create an iterator from the beginning of the topic
     * or from the last consumed offset.
     */
    Iterator begin() { return Iterator(shared_from_this()); }

    /**
     * @brief Create an iterator indicating the end of a topic.
     */
    Iterator end() { return Iterator(); }
    /**
     * @brief Destructor.
     */
    virtual ~ConsumerInterface() = default; // LCOV_EXCL_LINE

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
     * @brief Returns the DataAllocator used by the Consumer.
     */
    virtual const DataAllocator& dataAllocator() const = 0;

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
    friend struct PythonBindingHelper;

    public:

    inline Consumer() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    inline Consumer(const Consumer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline Consumer(Consumer&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    inline Consumer& operator=(const Consumer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Consumer& operator=(Consumer&&) = default; // LCOV_EXCL_LINE

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
     * @brief Returns the DataAllocator used by the Consumer.
     */
    inline decltype(auto) dataAllocator() const {
        return self->dataAllocator();
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
     {}*
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

    class Iterator {

        friend class Consumer;

        public:

        using iterator_category = std::forward_iterator_tag;
        using value_type = Event;
        using difference_type = std::ptrdiff_t;
        using pointer = Event*;
        using reference = Event&;

        inline Iterator() = default;

        inline reference operator*() const { return m_current_event; }

        inline pointer operator->() const { return &m_current_event; }

        inline Iterator& operator++() {
            m_current_event = m_owner->pull().wait();
            if(m_current_event.id() == NoMoreEvents) {
                m_owner = nullptr;
                m_current_event = Event{};
            }
            return *this;
        }

        inline bool operator==(const Iterator& other) const {
            if(!other.m_owner && !m_owner) return true;
            return false;
        }

        inline bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

        private:

        Iterator(std::shared_ptr<ConsumerInterface> owner)
        : m_owner(std::move(owner)) {
            ++(*this);
        }

        mutable Event                      m_current_event;
        std::shared_ptr<ConsumerInterface> m_owner;
    };

    /**
     * @brief Create an iterator from the beginning of the topic
     * or from the last consumed offset.
     */
    Iterator begin() { return Iterator(self); }

    /**
     * @brief Create an iterator indicating the end of a topic.
     */
    Iterator end() { return Iterator(); }

    private:

    inline Consumer(const std::shared_ptr<ConsumerInterface>& impl)
    : self{impl} {}

    std::shared_ptr<ConsumerInterface> self;
};

}

#endif
