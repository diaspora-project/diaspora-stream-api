/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_THREAD_POOL_HPP
#define MOFKA_API_THREAD_POOL_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Future.hpp>

#include <memory>
#include <functional>
#include <limits>

namespace mofka {

/**
 * @brief Strongly typped size_t meant to represent the number of
 * threads in a ThreadPool.
 */
struct ThreadCount {

    std::size_t count;

    explicit constexpr ThreadCount(std::size_t val)
    : count(val) {}
};

/**
 * @brief A ThreadPool is an object that manages a number of threads
 * and pushes works for them to execute.
 */
class ThreadPoolInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ThreadPoolInterface() = default;

    /**
     * @brief Returns the number of underlying threads.
     */
    virtual ThreadCount threadCount() const = 0;

    /**
     * @brief Push work into the thread pool.
     *
     * @param func Function to push.
     * @param priority Priority.
     */
    virtual void pushWork(std::function<void()> func,
                          uint64_t priority = std::numeric_limits<uint64_t>::max()) const = 0;

    /**
     * @brief Get the number of ULTs in the pool, including blocked and running ULTs.
     */
    size_t size() const;
};

/**
 * @brief A ThreadPool is an object encapsulating an implementation of a ThreadPoolInterface.
 */
class ThreadPool {

    public:

    /**
     * @brief Constructor.
     */
    ThreadPool() = default;

    /**
     * @brief Copy-constructor.
     */
    ThreadPool(const ThreadPool&) = default;

    /**
     * @brief Move-constructor.
     */
    ThreadPool(ThreadPool&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    ThreadPool& operator=(const ThreadPool&) = default;

    /**
     * @brief Move-assignment operator.
     */
    ThreadPool& operator=(ThreadPool&&) = default;

    /**
     * @brief Destructor.
     */
    ~ThreadPool() = default;

    /**
     * @brief Returns the number of underlying threads.
     */
    ThreadCount threadCount() const {
        return self->threadCount();
    }

    /**
     * @brief Push work into the thread pool.
     *
     * @param func Function to push.
     * @param priority Priority.
     */
    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) const {
        self->pushWork(std::move(func), priority);
    }

    /**
     * @brief Get the number of ULTs in the pool, including blocked and running ULTs.
     */
    size_t size() const;

    /**
     * @brief Checks if the ThreadPool instance is valid.
     */
    explicit operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a ThreadPool instance.
     *
     * @param impl Pointer to implementation.
     */
    ThreadPool(const std::shared_ptr<ThreadPoolInterface>& self);

    std::shared_ptr<ThreadPoolInterface> self;
};

}

#endif
