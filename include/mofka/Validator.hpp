/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_API_VALIDATOR_HPP
#define MOFKA_API_VALIDATOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataView.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Factory.hpp>
#include <mofka/InvalidMetadata.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The ValidatorInterface class provides an interface for
 * validating instances of the Metadata class.
 *
 * A ValidatorInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class ValidatorInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ValidatorInterface() = default;

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    virtual void validate(const Metadata& metadata, const DataView& data) const = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;


    /**
     * @note A ValidatorInterface class must also provide a static Create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<ValidatorInterface> create(const Metadata&);
     */
};

class Validator {

    friend struct PythonBindingHelper;

    public:

    /**
     * @brief Constructor. Will construct a valid Validator that accepts
     * any Metadata correctly formatted in JSON.
     */
    Validator();

    /**
     * @brief Copy-constructor.
     */
    inline Validator(const Validator&) = default;

    /**
     * @brief Move-constructor.
     */
    inline Validator(Validator&&) = default;

    /**
     * @brief copy-assignment operator.
     */
    inline Validator& operator=(const Validator&) = default;

    /**
     * @brief Move-assignment operator.
     */
    inline Validator& operator=(Validator&&) = default;

    /**
     * @brief Destructor.
     */
    inline ~Validator() = default;

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    inline explicit operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    inline void validate(const Metadata& metadata, const DataView& data) const {
        self->validate(metadata, data);
    }

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    inline Metadata metadata() const {
        return self->metadata();
    }

    /**
     * @brief Factory function to create a Validator instance.
     * The Metadata is expected to be a JSON object with at least
     * a "type" field. The type can be in the form "name:library.so"
     * if library.so must be loaded to access the validator.
     * If the "type" field is not provided, it is assumed to be
     * "default".
     *
     * @param metadata Metadata of the Validator.
     *
     * @return Validator instance.
     */
    static Validator FromMetadata(const Metadata& metadata);

    private:

    Validator(std::shared_ptr<ValidatorInterface> impl)
    : self{std::move(impl)} {}

    std::shared_ptr<ValidatorInterface> self;
};

using ValidatorFactory = Factory<ValidatorInterface, const Metadata&>;

#define MOFKA_REGISTER_VALIDATOR(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(ValidatorFactory, __type__, __name__)

}

#endif
