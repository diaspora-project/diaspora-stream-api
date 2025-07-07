/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_FUTURE_HPP
#define MOFKA_API_FUTURE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>

#include <memory>
#include <functional>

namespace mofka {

/**
 * @brief Future objects are used to keep track of
 * on-going asynchronous operations.
 */
template<typename ResultType,
         typename WaitFn = std::function<ResultType()>,
         typename TestFn = std::function<bool()>>
class Future {

    public:

    /**
     * @brief Default constructor. Will create a non-valid Future.
     */
    inline Future() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy constructor.
     */
    inline Future(const Future& other) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move constructor.
     */
    inline Future(Future&& other) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    inline Future& operator=(const Future& other) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Future& operator=(Future&& other) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~Future() = default; // LCOV_EXCL_LINE

    /**
     * @brief Check the validity of the Future.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(m_wait) || static_cast<bool>(m_completed);
    }

    /**
     * @brief Wait for the request to complete.
     */
    inline ResultType wait() const {
        if(!m_wait)
            throw Exception("Calling Future::wait on an invalid future");
        return m_wait();
    }

    /**
     * @brief Test if the request has completed, without blocking.
     */
    inline bool completed() const {
        if(!m_completed)
            throw Exception("Calling Future::completed on an invalid future");
        return m_completed();
    }

    /**
     * @brief Constructor meant for classes that actually know what the
     * internals of the future are.
     */
    inline Future(WaitFn wait_fn,
           TestFn completed_fn)
    : m_wait(std::move(wait_fn))
    , m_completed(std::move(completed_fn)) {}

    private:

    WaitFn m_wait;
    TestFn m_completed;

};

}

#endif
