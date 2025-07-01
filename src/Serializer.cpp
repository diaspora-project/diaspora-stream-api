/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Exception.hpp"
#include "mofka/Serializer.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultSerializer.hpp"
#include "SchemaSerializer.hpp"

namespace mofka {

MOFKA_REGISTER_SERIALIZER(default, DefaultSerializer);
MOFKA_REGISTER_SERIALIZER(schema, SchemaSerializer);

Serializer::Serializer()
: self(std::make_shared<DefaultSerializer>()) {}

Serializer Serializer::FromMetadata(const Metadata& metadata) {
    const auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
                "Cannot create Serializer from Metadata: "
                "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("type")) {
        return Serializer{};
    }
    auto& type = json["type"];
    if(!type.is_string()) {
        throw Exception(
                "Cannot create Serializer from Metadata: "
                "invalid \"type\" field in Metadata (expected string)");
    }
    const auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<SerializerInterface> s = SerializerFactory::create(type_str, metadata);
    return Serializer(std::move(s));
}

Serializer Serializer::FromMetadata(const char* type, const Metadata& metadata) {
    const auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
                "Cannot create Serializer from Metadata: "
                "invalid Metadata (expected JSON object)");
    }
    auto md_copy = metadata;
    md_copy.json()["type"] = type;
    std::shared_ptr<SerializerInterface> s = SerializerFactory::create(type, md_copy);
    return Serializer(std::move(s));
}

}
