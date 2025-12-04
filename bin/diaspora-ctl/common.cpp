/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "common.hpp"
#include <spdlog/spdlog.h>
#include <fstream>
#include <sstream>
#include <cctype>

namespace diaspora_ctl {

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

} // namespace diaspora_ctl
