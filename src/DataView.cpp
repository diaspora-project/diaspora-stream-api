/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/DataView.hpp"
#include "mofka/Exception.hpp"

#include "DataViewImpl.hpp"
#include "PimplUtil.hpp"

#include <cstring>
#include <iostream>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(DataView);

DataView::DataView(Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataViewImpl>(ctx, std::move(free_cb))) {}

DataView::DataView(void* ptr, size_t size, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataViewImpl>(ptr, size, ctx, std::move(free_cb))) {}

DataView::DataView(std::vector<Segment> segments, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataViewImpl>(std::move(segments), ctx, std::move(free_cb))) {}

const std::vector<DataView::Segment>& DataView::segments() const {
    return self->m_segments;
}

size_t DataView::size() const {
    return self->m_size;
}

DataView::Context DataView::context() const {
    return self->m_context;
}

void DataView::write(const char* data, size_t size, size_t offset) const {
    size_t off = 0;
    for(auto& seg : segments()) {
        if(offset >= seg.size) {
            offset -= seg.size;
            continue;
        }
        // current segment needs to be copied
        auto size_to_copy = std::min(size, seg.size);
        std::memcpy((char*)seg.ptr + offset, data + off, size_to_copy);
        offset = 0;
        off += size_to_copy;
        size -= size_to_copy;
        if(size == 0) break;
    }
}

}
