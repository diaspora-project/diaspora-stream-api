/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_NUM_EVENTS_HPP
#define MOFKA_API_NUM_EVENTS_HPP

#include <mofka/ForwardDcl.hpp>

#include <cstdint>

namespace mofka {

/**
 * @brief Strongly typped size_t meant to store a number of events.
 */
struct NumEvents {

    std::size_t value;

    explicit constexpr NumEvents(std::size_t val)
    : value(val) {}

    /**
     * @brief A value so large you are unlikely to see than many events
     * in the lifetime of the application.
     */
    static NumEvents Infinity();

    inline bool operator<(const NumEvents& other) const { return value < other.value; }
    inline bool operator>(const NumEvents& other) const { return value > other.value; }
    inline bool operator<=(const NumEvents& other) const { return value <= other.value; }
    inline bool operator>=(const NumEvents& other) const { return value >= other.value; }
    inline bool operator==(const NumEvents& other) const { return value == other.value; }
    inline bool operator!=(const NumEvents& other) const { return value != other.value; }
};

}

#endif
