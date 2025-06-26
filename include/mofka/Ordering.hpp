/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_ORDERING_HPP
#define MOFKA_API_ORDERING_HPP

#include <mofka/ForwardDcl.hpp>

namespace mofka {

enum class Ordering : bool {
    Strict = true, /* events must be stored in the same order they are produced */
    Loose  = false /* events can be stored in a different order */
};

}

#endif
