/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DATA_DESCRIPTOR_HPP
#define DIASPORA_API_DATA_DESCRIPTOR_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Archive.hpp>

#include <vector>
#include <string_view>
#include <variant>
#include <cstring>

namespace diaspora {

/**
 * @brief A DataDescriptor is an opaque object describing
 * how the data is stored in some storage backend in Diaspora.
 */
class DataDescriptor {

    public:

    /**
     * @brief represents a contiguous segment of data.
     */
    struct Segment {
        std::size_t offset;
        std::size_t size;
    };

    using Contiguous = Segment;

    /**
     * @brief Represents a strided selection, starting at a given
     * offset, counting numblocks blocks, each with a given blocksize
     * and separated from the next block by gapsize.
     */
    struct Strided {
        std::size_t offset;
        std::size_t numblocks;
        std::size_t blocksize;
        std::size_t gapsize;
    };

    /**
     * @brief Arbitrary selection of the underlying data as a series
     * of segments.
     */
    struct Unstructured {
        std::vector<Segment> segments;
    };

    using Selection = std::variant<Contiguous, Strided, Unstructured>;

    /**
     * @brief Create an implementation-dependent DataDescriptor.
     *
     * @param opaque Implementation-dependent representation of the data location.
     * @param size Size of the underlying data.
     *
     * @return a DataDescriptor.
     */
    DataDescriptor(std::string_view opaque, size_t size)
    : m_location{opaque.data(), opaque.size()}
    , m_size(size)
    , m_base_size(size) {}

    /**
     * @brief Constructor (equivalent to a DataDescriptor for no data).
     */
    DataDescriptor() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    DataDescriptor(const DataDescriptor&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    DataDescriptor(DataDescriptor&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    DataDescriptor& operator=(const DataDescriptor&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    DataDescriptor& operator=(DataDescriptor&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    ~DataDescriptor() = default; // LCOV_EXCL_LINE

    /**
     * @brief Return the size of the underlying data in bytes.
     */
    size_t size() const {
        return m_size;
    }

    /**
     * @brief Returns the location (interpretable by the backend).
     */
    const std::string& location() const {
        return m_location;
    }

    /**
     * @brief Returns the location (interpretable by the backend).
     */
    std::string& location() {
        return m_location;
    }

    /**
     * @brief Returns the stacked selections.
     */
    const auto& selections() const {
        return m_selections;
    }

    /**
     * @brief Extract a flat representation of the data descriptor.
     */
    std::vector<Segment> flatten() const;

    /**
     * @brief Create a DataDescriptor representing a subset of
     * the data represented by this descriptor.
     *
     * @param offset Offset at which to start the view.
     * @param numblocks Number of blocks to take.
     * @param blocksize Size of each block.
     * @param gapsize Distance between the end of a block
     * and the beginning of the next one.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *   **   **   **   **   **
     *
     * Calling D.makeStridedView(1, 5, 2, 3) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "BCGHLMQRVW"
     *
     * We have 5 blocks of length 2 with a gap of 3 between each block,
     * starting at an offset of 1 byte.
     */
    DataDescriptor makeStridedView(
        size_t offset,
        size_t numblocks,
        size_t blocksize,
        size_t gapsize) const;

    /**
     * @brief This function takes a subset of the initial DataDescriptor
     * by selecting a contiguous segment of the specified size starting
     * at the specified offset.
     *
     * @param offset Offset of the view.
     * @param size Size of the view.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *      ********
     *
     * Calling D.makeSubView(4, 8) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "EFGHIJKL"
     */
    DataDescriptor makeSubView(
        size_t offset,
        size_t size) const;

    /**
     * @brief This function takes a map associating an offset to a size
     * and creates a view by selecting the segments (offset, size) in
     * the underlying DataDescriptor.
     *
     * @warning: segments must not overlap and must come in order.
     *
     * @note: the use of an std::map forces segments to be sorted by offset.
     *
     * @note: an unstructured DataDescriptor is more difficult to
     * handle and store than a structured (sub or strided) one, so do not
     * use this function if you have the possibility to use sub or strided
     * views (or a composition of them).
     *
     * @param segments List of contiguous segments.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *   ***   *****  **
     *
     * Let M be a list with the content {{1, 3}, {7, 5}, {14, 2}}.
     * Calling D.makeUnstructuredView(M) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "BCDHIJKLOPQR"
     */
    DataDescriptor makeUnstructuredView(
        const std::vector<Contiguous>& segments) const;


    /**
     * @brief Load the DataDescriptor from an Archive.
     *
     * @param ar Archive.
     */
    void load(Archive& ar);

    /**
     * @brief Serialize the DataDescriptor into an Archive.
     *
     * @param ar Archive.
     */
    void save(Archive& ar) const;

    private:

    std::string            m_location;   /* implementation defined data location */
    std::vector<Selection> m_selections; /* stack of selections on top of the data */
    size_t                 m_size = 0;   /* size of the data after selections applied */
    size_t                 m_base_size = 0; /* size of the data on which the selection is applied */
};

}

#endif
