/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_SERIALIZER_HPP
#define MOFKA_API_SERIALIZER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Archive.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/InvalidMetadata.hpp>
#include <mofka/Factory.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The SerializerInterface class provides an interface for
 * serializing the Metadata class. Its serialize and deserialize
 * methods should make use of the Archive's write and read methods
 * respectively.
 *
 * A SerializerInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class SerializerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~SerializerInterface() = default;

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    virtual void serialize(Archive& archive, const Metadata& metadata) const = 0;

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    virtual void deserialize(Archive& archive, Metadata& metadata) const = 0;

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    virtual Metadata metadata() const = 0;

    /**
     * @note A SerializerInterface class must also provide a static create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::unique_ptr<SerializerInterface> create(const Metadata&);
     */
};

class Serializer {

    friend struct PythonBindingHelper;

    public:

    /**
     * @brief Constructor. Will construct a valid Serializer that simply
     * serializes the Metadata into JSON string.
     */
    Serializer();

    /**
     * @brief Copy-constructor.
     */
    inline Serializer(const Serializer&) = default;

    /**
     * @brief Move-constructor.
     */
    inline Serializer(Serializer&&) = default;

    /**
     * @brief copy-assignment operator.
     */
    inline Serializer& operator=(const Serializer&) = default;

    /**
     * @brief Move-assignment operator.
     */
    inline Serializer& operator=(Serializer&&) = default;

    /**
     * @brief Destructor.
     */
    inline ~Serializer() = default;

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    inline void serialize(Archive& archive, const Metadata& metadata) const {
        self->serialize(archive, metadata);
    }

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    inline void deserialize(Archive& archive, Metadata& metadata) const {
        self->deserialize(archive, metadata);
    }

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    inline Metadata metadata() const {
        return self->metadata();
    }

    /**
     * @brief Factory function to create a Serializer instance.
     *
     * @param type Type of Serializer.
     * @param metadata Metadata of the Serializer.
     *
     * @return Serializer instance.
     */
    static Serializer FromMetadata(
            const char* type,
            const Metadata& metadata);

    /**
     * @brief Same as the above function but the type is expected
     * to be provided as a "__type__" field in the metdata, and the
     * function will fall back to "default" if not provided.
     *
     * @param metadata Metadata of the Serializer.
     *
     * @return Serializer instance.
     */
    static Serializer FromMetadata(const Metadata& metadata);

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    Serializer(const std::shared_ptr<SerializerInterface>& impl)
    : self(impl) {}

    std::shared_ptr<SerializerInterface> self;

};

using SerializerFactory = Factory<SerializerInterface, const Metadata&>;

}

#define MOFKA_REGISTER_SERIALIZER(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(SerializerFactory, __type__, __name__)

#endif
