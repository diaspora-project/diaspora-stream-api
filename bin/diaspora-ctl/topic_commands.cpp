/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "topic_commands.hpp"
#include "common.hpp"
#include <diaspora/Driver.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace diaspora_ctl {

int topic_create(int argc, char** argv) {
    try {
        // Extract --driver.* and --topic.* metadata arguments before TCLAP parsing
        auto parsed_args = extract_metadata_args(argc, argv);

        // Prepare arguments for TCLAP
        int filtered_argc = parsed_args.filtered_argv.size();
        char** filtered_argv = parsed_args.filtered_argv.data();

        TCLAP::CmdLine cmd("Create a new topic", ' ', "1.0");

        TCLAP::ValueArg<std::string> driverArg("", "driver", "Driver name", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> driverConfigArg("", "driver-config", "Driver config file", false, "", "filename", cmd);
        TCLAP::ValueArg<std::string> nameArg("", "name", "Topic name", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> topicConfigArg("", "topic-config", "Topic config file", false, "", "filename", cmd);
        TCLAP::ValueArg<std::string> serializerArg("", "serializer", "Serializer name", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> validatorArg("", "validator", "Validator name", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> partitionSelectorArg("", "partition-selector", "Partition selector name", false, "", "string", cmd);

        std::vector<std::string> allowed_levels{"trace", "debug", "info", "warn", "error", "critical", "off"};
        TCLAP::ValuesConstraint<std::string> level_constraint(allowed_levels);
        TCLAP::ValueArg<std::string> loggingArg("", "logging", "Logging level", false, "info", &level_constraint, cmd);

        cmd.parse(filtered_argc, filtered_argv);

        // Set logging level
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        // Read configuration files
        std::string driver_config_str = read_config_file(driverConfigArg.getValue());
        std::string topic_config_str = read_config_file(topicConfigArg.getValue());

        // Parse config files as JSON
        nlohmann::json driver_config = nlohmann::json::parse(driver_config_str);
        nlohmann::json topic_config = nlohmann::json::parse(topic_config_str);

        // Merge command-line metadata with config file metadata
        // Command-line arguments take precedence
        driver_config.merge_patch(parsed_args.driver_metadata);
        topic_config.merge_patch(parsed_args.topic_metadata);

        spdlog::debug("Driver config: {}", driver_config.dump());
        spdlog::debug("Topic config: {}", topic_config.dump());

        spdlog::info("Creating driver: {}", driverArg.getValue());
        auto driver = diaspora::Driver::New(driverArg.getValue().c_str(), diaspora::Metadata{driver_config.dump()});

        spdlog::info("Creating topic: {}", nameArg.getValue());

        // TODO: Handle validator, serializer, and partition selector creation
        // For now, these are passed as nullptr (optional parameters)
        diaspora::Validator validator;
        diaspora::Serializer serializer;
        diaspora::PartitionSelector selector;

        if (!validatorArg.getValue().empty()) {
            spdlog::warn("Validator parameter provided but not yet implemented");
        }
        if (!serializerArg.getValue().empty()) {
            spdlog::warn("Serializer parameter provided but not yet implemented");
        }
        if (!partitionSelectorArg.getValue().empty()) {
            spdlog::warn("Partition selector parameter provided but not yet implemented");
        }

        driver.createTopic(
            nameArg.getValue(),
            diaspora::Metadata{topic_config.dump()},
            validator,
            selector,
            serializer);

        spdlog::info("Topic '{}' created successfully", nameArg.getValue());
        return 0;

    } catch (TCLAP::ArgException& e) {
        spdlog::error("Argument error: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
        return 1;
    }
}

} // namespace diaspora_ctl
