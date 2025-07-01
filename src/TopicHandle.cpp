/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Driver.hpp"
#include "PimplUtil.hpp"
#include <limits>

namespace mofka {

Driver TopicHandle::driver() const {
    return Driver(self->driver());
}

}
