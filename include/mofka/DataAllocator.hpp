/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_DATA_ALLOCATOR_HPP
#define MOFKA_API_DATA_ALLOCATOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataView.hpp>
#include <mofka/DataDescriptor.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief DataAllocator is the type of a function that takes the
 * Metadata of an event as well as the DataDescriptor of the associated
 * data, and returns a Data object indicating where in memory the data
 * of the event should be placed by the Consumer.
 */
using DataAllocator = std::function<DataView(const Metadata&, const DataDescriptor&)>;

}

#endif
