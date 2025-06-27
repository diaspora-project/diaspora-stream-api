/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Exception.hpp"
#include "mofka/PartitionSelector.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultPartitionSelector.hpp"
#include <unordered_map>

namespace mofka {

PartitionSelector::PartitionSelector()
: self(std::make_shared<DefaultPartitionSelector>()) {}

MOFKA_REGISTER_PARTITION_SELECTOR(default, DefaultPartitionSelector);

PartitionSelector PartitionSelector::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("type")) {
        return PartitionSelector{};
    }
    auto& type = json["type"];
    if(!type.is_string()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid \"type\" field in Metadata (expected string)");
    }
    auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<PartitionSelectorInterface> ts = PartitionSelectorFactory::create(type_str, metadata);
    return ts;
}

PartitionSelector PartitionSelector::FromMetadata(const char* type, const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    auto md_copy = metadata;
    md_copy.json()["type"] = type;
    std::shared_ptr<PartitionSelectorInterface> ts = PartitionSelectorFactory::create(type, md_copy);
    return ts;
}

}
