/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"
#include "PimplUtil.hpp"
#include <limits>

namespace mofka {

TopicHandle Producer::topic() const {
    return TopicHandle(self->topic());
}

}
