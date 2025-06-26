/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/ThreadPool.hpp"
#include "ThreadPoolImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(ThreadPool);

ThreadCount ThreadPool::threadCount() const{
    return mofka::ThreadCount{self->managed_xstreams_size()};
}

size_t ThreadPool::size() const {
    return self->size();
}

}
