/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_DATA_VIEW_HPP
#define MOFKA_API_DATA_VIEW_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>

#include <functional>
#include <memory>
#include <vector>

namespace mofka {

class DataViewImpl;

/**
 * @brief A DataView is an object that encapsulates the data of an event.
 */
class DataView {

    public:

    struct Segment {
        void*  ptr;
        size_t size;
    };

    using Context = void*;
    using FreeCallback = std::function<void(Context)>;

    /**
     * @brief Constructor. The resulting Data handle will represent NULL.
     */
    DataView(Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Creates a Data object with a single segment.
     */
    DataView(void* ptr, size_t size, Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Creates a Data object from a list of Segments.
     */
    DataView(std::vector<Segment> segments, Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Copy-constructor.
     */
    DataView(const DataView&);

    /**
     * @brief Move-constructor.
     */
    DataView(DataView&&);

    /**
     * @brief Copy-assignment operator.
     */
    DataView& operator=(const DataView&);

    /**
     * @brief Move-assignment operator.
     */
    DataView& operator=(DataView&&);

    /**
     * @brief Free.
     */
    ~DataView();

    /**
     * @brief Returns the list of memory segments
     * this Data object refers to.
     */
    const std::vector<Segment>& segments() const;

    /**
     * @brief Return the total size of the Data.
     */
    size_t size() const;

    /**
     * @brief Write the content of target at the specified offset
     * into the memory represented by this Data object.
     *
     * @param data Data to write.
     * @param size Size of the data.
     * @param from_offset Offset from which to write.
     */
    void write(const char* data, size_t size, size_t offset = 0) const;

    /**
     * @brief Checks if the Data instance is valid.
     */
    explicit operator bool() const;

    /**
     * @brief Return the context of this Data object.
     */
    Context context() const;

    private:

    /**
     * @brief Constructor is private.
     *
     * @param impl Pointer to implementation.
     */
    DataView(const std::shared_ptr<DataViewImpl>& impl);

    std::shared_ptr<DataViewImpl> self;

    friend class Event;

};

}

#endif
