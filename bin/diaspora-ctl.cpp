#include <diaspora/Driver.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>

std::string read_config_file(const std::string& filename) {
    if (filename.empty()) {
        return "{}";
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        spdlog::error("Failed to open config file: {}", filename);
        return "{}";
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

bool is_number(const std::string& s) {
    if (s.empty()) return false;

    size_t start = 0;
    if (s[0] == '-' || s[0] == '+') start = 1;
    if (start >= s.length()) return false;

    bool has_dot = false;
    for (size_t i = start; i < s.length(); ++i) {
        if (s[i] == '.') {
            if (has_dot) return false; // Multiple dots
            has_dot = true;
        } else if (!std::isdigit(s[i])) {
            return false;
        }
    }
    return true;
}

nlohmann::json parse_value(const std::string& value) {
    // Check if it's a number
    if (is_number(value)) {
        if (value.find('.') != std::string::npos) {
            return std::stod(value);
        } else {
            return std::stoll(value);
        }
    }

    // Check if it's a comma-separated list (array)
    if (value.find(',') != std::string::npos) {
        nlohmann::json arr = nlohmann::json::array();
        std::stringstream ss(value);
        std::string item;
        while (std::getline(ss, item, ',')) {
            arr.push_back(parse_value(item));
        }
        return arr;
    }

    // Otherwise, it's a string
    return value;
}

void set_nested_value(nlohmann::json& obj, const std::string& key, const nlohmann::json& value) {
    // Split key by dots to support nested configuration
    std::vector<std::string> parts;
    std::stringstream ss(key);
    std::string part;
    while (std::getline(ss, part, '.')) {
        parts.push_back(part);
    }

    // Navigate to the nested location
    nlohmann::json* current = &obj;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        const auto& part_key = parts[i];
        if (!current->contains(part_key) || !(*current)[part_key].is_object()) {
            (*current)[part_key] = nlohmann::json::object();
        }
        current = &(*current)[part_key];
    }

    // Set the final value
    (*current)[parts.back()] = value;
}

struct ParsedArgs {
    nlohmann::json driver_metadata;
    nlohmann::json topic_metadata;
    std::vector<char*> filtered_argv;
};

ParsedArgs extract_metadata_args(int argc, char** argv) {
    ParsedArgs result;
    result.driver_metadata = nlohmann::json::object();
    result.topic_metadata = nlohmann::json::object();

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg.rfind("--driver.", 0) == 0) {
            // Extract key after "--driver."
            std::string key = arg.substr(9);
            if (i + 1 < argc) {
                std::string value = argv[i + 1];
                set_nested_value(result.driver_metadata, key, parse_value(value));
                i++; // Skip the value argument
                continue;
            }
        } else if (arg.rfind("--topic.", 0) == 0) {
            // Extract key after "--topic."
            std::string key = arg.substr(8);
            if (i + 1 < argc) {
                std::string value = argv[i + 1];
                set_nested_value(result.topic_metadata, key, parse_value(value));
                i++; // Skip the value argument
                continue;
            }
        }

        result.filtered_argv.push_back(argv[i]);
    }

    return result;
}

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

void print_usage() {
    std::cout << "Usage: diaspora-ctl <command> <action> [options]\n\n";
    std::cout << "Commands:\n";
    std::cout << "  topic create    Create a new topic\n";
    std::cout << "\nFor help on a specific command, use:\n";
    std::cout << "  diaspora-ctl topic create --help\n";
}

int main(int argc, char** argv) {
    // Configure spdlog
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");

    if (argc < 3) {
        print_usage();
        return 1;
    }

    std::string command = argv[1];
    std::string action = argv[2];

    if (command == "topic" && action == "create") {
        // Remove the first two arguments (command and action)
        return topic_create(argc - 2, argv + 2);
    } else {
        spdlog::error("Unknown command: {} {}", command, action);
        print_usage();
        return 1;
    }

    return 0;
}
