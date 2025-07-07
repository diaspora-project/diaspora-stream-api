/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_EXCEPTION_HPP
#define MOFKA_API_EXCEPTION_HPP

#include <mofka/ForwardDcl.hpp>

#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief Exception class to use in Mofka backend implementations.
 */
class Exception : public std::logic_error {

    public:

    Exception(const Exception&) = default; // LCOV_EXCL_LINE

    Exception(Exception&&) = default; // LCOV_EXCL_LINE

    Exception& operator=(const Exception&) = default; // LCOV_EXCL_LINE

    Exception& operator=(Exception&&) = default; // LCOV_EXCL_LINE

    Exception(const char* w)
    : std::logic_error(w) {}

    Exception(const std::string& w)
    : std::logic_error(w) {}
};

}

#endif
