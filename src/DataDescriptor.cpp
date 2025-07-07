/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/DataDescriptor.hpp"
#include "mofka/Exception.hpp"

#include <cmath>

namespace mofka {


std::vector<DataDescriptor::Segment> DataDescriptor::flatten() const {
    std::vector<Segment> result;

    // Start with the base segment representing the entire memory region
    result.push_back({0, std::numeric_limits<size_t>::max()});

    // Temporary result vector used in each iteration
    std::vector<Segment> temp;

    // Process each selection in order from bottom to top (vector[0] is bottom of stack)
    for (const Selection& sel : m_selections) {

        temp.clear();

        if (std::holds_alternative<Segment>(sel)) {
            const Segment& s = std::get<Segment>(sel);
            for (const Segment& seg : result) {
                size_t seg_end = seg.offset + seg.size;
                size_t s_end = s.offset + s.size;

                if (s.offset >= seg_end || s_end <= seg.offset) {
                    continue; // No overlap
                }

                size_t new_offset = std::max(seg.offset, s.offset);
                size_t new_end = std::min(seg_end, s_end);
                temp.push_back({new_offset, new_end - new_offset});
            }

        } else if (std::holds_alternative<Strided>(sel)) {
            const Strided& s = std::get<Strided>(sel);
            for (const Segment& seg : result) {
                size_t seg_end = seg.offset + seg.size;

                for (size_t i = 0; i < s.numblocks; ++i) {
                    size_t block_start = s.offset + i * (s.blocksize + s.gapsize);
                    size_t block_end = block_start + s.blocksize;

                    if (block_end <= seg.offset || block_start >= seg_end) {
                        continue; // Block outside segment
                    }

                    size_t new_offset = std::max(seg.offset, block_start);
                    size_t new_end = std::min(seg_end, block_end);
                    if (new_end > new_offset) {
                        temp.push_back({new_offset, new_end - new_offset});
                    }
                }
            }

        } else if (std::holds_alternative<Unstructured>(sel)) {
            const Unstructured& u = std::get<Unstructured>(sel);
            for (const Segment& user_seg : u.segments) {
                size_t user_seg_end = user_seg.offset + user_seg.size;

                for (const Segment& seg : result) {
                    size_t seg_end = seg.offset + seg.size;

                    if (user_seg.offset >= seg_end || user_seg_end <= seg.offset) {
                        continue; // No overlap
                    }

                    size_t new_offset = std::max(seg.offset, user_seg.offset);
                    size_t new_end = std::min(seg_end, user_seg_end);
                    temp.push_back({new_offset, new_end - new_offset});
                }
            }
        }

        result = temp;
    }

    // Normalize offsets relative to the first segment's offset, so the final view starts at 0
    if (!result.empty()) {
        size_t base_offset = result.front().offset;
        for (Segment& s : result) {
            s.offset -= base_offset;
        }
    }

    return result;
}

DataDescriptor DataDescriptor::makeStridedView(
        size_t offset,
        size_t numblocks,
        size_t blocksize,
        size_t gapsize) const {
    if(offset > m_size || numblocks == 0 || blocksize == 0)
        return DataDescriptor();
    // check that the stride doesn't exceeds the available size
    if(offset + numblocks*(blocksize + gapsize) > m_size)
        throw Exception{"Invalid strided view: would go out of bounds"};

    // make the new descriptor
    auto newDesc = *this;
    newDesc.m_selections.emplace_back(Strided{offset, numblocks, blocksize, gapsize});
    newDesc.m_size = numblocks*blocksize;
    // TODO optimize further the content of the new descriptor
    return newDesc;
}

DataDescriptor DataDescriptor::makeSubView(
        size_t offset,
        size_t size) const {
    if(offset > m_size || size == 0 || m_size == 0)
        return DataDescriptor();
    // make the new descriptor
    auto newDesc = *this;
    size = std::min(newDesc.m_size - offset, size);
    newDesc.m_selections.emplace_back(Contiguous{offset, size});
    newDesc.m_size = size;
    // TODO optimize further the content of the new descriptor
    return newDesc;
}


DataDescriptor DataDescriptor::makeUnstructuredView(
        const std::vector<Contiguous>& segments) const {
    if(segments.empty()) return DataDescriptor();
    auto newDesc = *this;
    size_t view_size = 0;
    size_t current_offset = 0;
    Unstructured u;
    for(auto& [offset, size] : segments) {
        if(offset < current_offset)
            throw Exception("Invalid unstructured view: segments overlapping or out of order");
        if((offset >= m_size) || (offset + size > m_size))
            throw Exception("Invalid unstructured view: would go out of bounds");
        if(!u.segments.empty() && u.segments.back().offset + u.segments.back().size == offset) {
            u.segments.back().size += size;
            view_size += size;
            current_offset = offset + size;
        } else {
            u.segments.emplace_back(Segment{offset, size});
            view_size += size;
            current_offset = offset + size;
        }
    }
    if(u.segments.size() == 0)
        return DataDescriptor();
    if(u.segments.size() == 1)
        return makeSubView(u.segments[0].offset, u.segments[0].size);
    newDesc.m_size = view_size;
    newDesc.m_selections.emplace_back(std::move(u));
    return newDesc;
}

}
