/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_VIEW_IMPL_HPP
#define MOFKA_DATA_VIEW_IMPL_HPP

#include "mofka/DataView.hpp"
#include <numeric>

namespace mofka {

class DataViewImpl {

    public:

    DataViewImpl(std::vector<DataView::Segment> segments,
                 DataView::Context ctx = nullptr,
                 DataView::FreeCallback&& free_cb = DataView::FreeCallback{})
    : m_segments(std::move(segments))
    , m_context{ctx}
    , m_free{std::move(free_cb)} {
        for(auto& s : m_segments) {
            m_size += s.size;
        }
    }

    DataViewImpl(void* ptr, size_t size,
                 DataView::Context ctx = nullptr,
                 DataView::FreeCallback&& free_cb = DataView::FreeCallback{})
    : m_segments{{ptr, size}}
    , m_size(size)
    , m_context{ctx}
    , m_free{free_cb} {}

    DataViewImpl(DataView::Context ctx = nullptr,
                 DataView::FreeCallback&& free_cb = DataView::FreeCallback{})
    : m_context{ctx}
    , m_free{std::move(free_cb)} {}

    ~DataViewImpl() {
        if(m_free) m_free(m_context);
    }

    std::vector<DataView::Segment> m_segments;
    size_t                         m_size = 0;
    DataView::Context              m_context = nullptr;
    DataView::FreeCallback         m_free;
};

}

#endif
