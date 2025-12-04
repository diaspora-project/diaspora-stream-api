/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

/**
 * @file diaspora-fifo.cpp
 * @brief Diaspora Stream FIFO Daemon
 *
 * This program provides a command-line interface for running a Diaspora stream
 * daemon that can be controlled via a control file.
 */

#include <diaspora/Driver.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/TopicHandle.hpp>

#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <errno.h>
#include <cstring>

// Global flag for signal handling
static std::atomic<bool> g_shutdown_requested{false};

/**
 * @brief Signal handler for graceful shutdown
 */
void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        spdlog::info("Received shutdown signal...");
        g_shutdown_requested = true;
    }
}

/**
 * @brief Load driver configuration from a JSON file
 *
 * @param config_path Path to the JSON configuration file
 * @return Metadata object containing the configuration
 */
diaspora::Metadata load_driver_config(const std::string& config_path) {
    try {
        std::ifstream file(config_path);
        if (!file.is_open()) {
            spdlog::error("Driver config file not found: {}", config_path);
            std::exit(1);
        }

        std::string content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());

        return diaspora::Metadata{content};
    } catch (const diaspora::Exception& e) {
        spdlog::error("Invalid JSON in driver config file: {}", e.what());
        std::exit(1);
    } catch (const std::exception& e) {
        spdlog::error("Failed to read config file: {}", e.what());
        std::exit(1);
    }
}

/**
 * @brief Create and initialize a Diaspora driver
 *
 * @param driver_name Name of the driver (e.g., "simple:libdiaspora-simple-backend.so")
 * @param driver_config Configuration metadata for the driver
 * @return Driver instance
 */
diaspora::Driver create_driver(const std::string& driver_name,
                                const diaspora::Metadata& driver_config) {
    try {
        auto driver = diaspora::Driver::New(driver_name.c_str(), driver_config);
        if (!driver) {
            spdlog::error("Failed to create driver '{}'", driver_name);
            std::exit(1);
        }
        return driver;
    } catch (const diaspora::Exception& e) {
        spdlog::error("Failed to create driver '{}': {}", driver_name, e.what());
        std::exit(1);
    } catch (const std::exception& e) {
        spdlog::error("Unexpected error creating driver: {}", e.what());
        std::exit(1);
    }
}

/**
 * @brief Parse a control command in the format "<fifo-name> -> <topic-name> (key1=value1, key2=value2, ...)"
 *
 * @param command The command string to parse
 * @param fifo_name Output parameter for the FIFO name
 * @param topic_name Output parameter for the topic name
 * @param options Output parameter for the key-value pairs
 * @return true if parsing succeeded, false otherwise
 */
bool parse_control_command(const std::string& command,
                           std::string& fifo_name,
                           std::string& topic_name,
                           std::unordered_map<std::string, std::string>& options) {
    // Helper function to trim whitespace
    auto trim = [](std::string& s) {
        s.erase(0, s.find_first_not_of(" \t\r\n"));
        s.erase(s.find_last_not_of(" \t\r\n") + 1);
    };

    // Find the arrow separator
    auto arrow_pos = command.find(" -> ");
    if (arrow_pos == std::string::npos) {
        return false;
    }

    fifo_name = command.substr(0, arrow_pos);
    trim(fifo_name);

    std::string rest = command.substr(arrow_pos + 4);
    trim(rest);

    // Check for optional key-value pairs in parentheses
    auto paren_pos = rest.find('(');
    if (paren_pos != std::string::npos) {
        // Extract topic name before the parenthesis
        topic_name = rest.substr(0, paren_pos);
        trim(topic_name);

        // Extract and parse key-value pairs
        auto close_paren_pos = rest.find(')', paren_pos);
        if (close_paren_pos == std::string::npos) {
            spdlog::warn("Missing closing parenthesis in command");
            return false;
        }

        std::string options_str = rest.substr(paren_pos + 1, close_paren_pos - paren_pos - 1);
        trim(options_str);

        if (!options_str.empty()) {
            // Split on commas
            std::stringstream ss(options_str);
            std::string pair;
            while (std::getline(ss, pair, ',')) {
                trim(pair);

                // Split on equals sign
                auto eq_pos = pair.find('=');
                if (eq_pos != std::string::npos) {
                    std::string key = pair.substr(0, eq_pos);
                    std::string value = pair.substr(eq_pos + 1);
                    trim(key);
                    trim(value);

                    if (!key.empty()) {
                        options[key] = value;
                    }
                } else {
                    spdlog::warn("Invalid key-value pair (missing '='): '{}'", pair);
                }
            }
        }
    } else {
        // No parentheses, just topic name
        topic_name = rest;
        trim(topic_name);
    }

    return !fifo_name.empty() && !topic_name.empty();
}

/**
 * @brief Structure to hold information about a producer FIFO
 */
struct ProducerInfo {
    int fd;
    std::string fifo_path;
    std::string topic_name;
    diaspora::Producer producer;
    std::string read_buffer;
    std::unordered_map<std::string, std::string> options;
};

/**
 * @brief Initialize and open the control FIFO
 *
 * @param control_file Path to the control FIFO
 * @return File descriptor for the control FIFO, or -1 on error
 */
int initialize_control_fifo(const std::string& control_file) {
    namespace fs = std::filesystem;

    fs::path control_path(control_file);

    // Ensure the directory exists
    if (control_path.has_parent_path()) {
        fs::create_directories(control_path.parent_path());
    }

    // Remove existing FIFO if present
    if (fs::exists(control_path)) {
        fs::remove(control_path);
    }

    // Create the control FIFO
    if (mkfifo(control_file.c_str(), 0666) == -1) {
        spdlog::error("Failed to create control FIFO: {}", std::strerror(errno));
        return -1;
    }
    spdlog::info("Created control FIFO: {}", control_file);

    // Open control FIFO for reading (non-blocking initially)
    int control_fd = open(control_file.c_str(), O_RDONLY | O_NONBLOCK);
    if (control_fd == -1) {
        spdlog::error("Failed to open control FIFO: {}", std::strerror(errno));
        return -1;
    }

    // Make it blocking after opening
    int flags = fcntl(control_fd, F_GETFL);
    fcntl(control_fd, F_SETFL, flags & ~O_NONBLOCK);

    return control_fd;
}

/**
 * @brief Handle a control command and create a producer
 *
 * @param driver The Diaspora driver instance
 * @param fifo_name The FIFO name for the producer
 * @param topic_name The topic name
 * @param options Options for the producer
 * @param topics Cache of topic handles
 * @param producers Map of existing producers
 * @return true if producer was created successfully
 */
bool handle_control_command(
    diaspora::Driver& driver,
    const std::string& fifo_name,
    const std::string& topic_name,
    const std::unordered_map<std::string, std::string>& options,
    std::unordered_map<std::string, diaspora::TopicHandle>& topics,
    std::unordered_map<std::string, ProducerInfo>& producers) {

    namespace fs = std::filesystem;

    // Check if this FIFO is already registered
    if (producers.find(fifo_name) != producers.end()) {
        spdlog::warn("FIFO '{}' already registered", fifo_name);
        return false;
    }

    try {
        // Open the topic
        diaspora::TopicHandle topic;
        if (topics.count(topic_name)) {
            topic = topics[topic_name];
        } else {
            topic = driver.openTopic(topic_name);
            topics[topic_name] = topic;
        }

        // Create a producer for this topic
        auto producer = topic.producer();

        // Create the FIFO if it doesn't exist
        fs::path fifo_path(fifo_name);
        if (!fs::exists(fifo_path)) {
            if (mkfifo(fifo_name.c_str(), 0666) == -1) {
                spdlog::error("Failed to create FIFO '{}': {}", fifo_name, std::strerror(errno));
                return false;
            }
            spdlog::info("Created FIFO: {}", fifo_name);
        }

        // Open the FIFO for reading (non-blocking)
        int fifo_fd = open(fifo_name.c_str(), O_RDONLY | O_NONBLOCK);
        if (fifo_fd == -1) {
            spdlog::error("Failed to open FIFO '{}': {}", fifo_name, std::strerror(errno));
            return false;
        }

        // Store the producer info
        producers[fifo_name] = ProducerInfo{
            fifo_fd,
            fifo_name,
            topic_name,
            producer,
            "",
            options
        };

        spdlog::info("Registered producer: {} -> {}", fifo_name, topic_name);
        return true;

    } catch (const diaspora::Exception& e) {
        spdlog::error("Failed to create producer: {}", e.what());
        return false;
    }
}

/**
 * @brief Process data from a producer FIFO
 *
 * @param info Producer information
 * @return true if data was processed, false if EOF or error
 */
bool handle_producer_data(ProducerInfo& info) {
    char buffer[4096];
    ssize_t n = read(info.fd, buffer, sizeof(buffer));

    if (n > 0) {
        info.read_buffer.append(buffer, n);

        // Process complete lines
        size_t pos;
        while ((pos = info.read_buffer.find('\n')) != std::string::npos) {
            std::string line = info.read_buffer.substr(0, pos);
            info.read_buffer.erase(0, pos + 1);

            if (!line.empty()) {
                try {
                    // Push the line as metadata to the producer
                    diaspora::Metadata metadata(line);
                    info.producer.push(metadata);
                    spdlog::debug("Pushed to '{}': {}", info.topic_name, line);
                } catch (const std::exception& e) {
                    spdlog::error("Failed to push event: {}", e.what());
                }
            }
        }
        return true;
    } else if (n == 0) {
        // EOF - writer closed the FIFO
        spdlog::info("Writer closed FIFO: {}", info.fifo_path);
        return false;
    } else {
        // Read error
        spdlog::error("Read error on FIFO '{}': {}", info.fifo_path, std::strerror(errno));
        return false;
    }
}

/**
 * @brief Clean up a single producer
 *
 * @param info Producer information to clean up
 */
void cleanup_producer(ProducerInfo& info) {
    namespace fs = std::filesystem;

    spdlog::info("Closing and removing producer for FIFO: {}", info.fifo_path);

    // Close the file descriptor
    close(info.fd);

    // Remove the FIFO file
    try {
        fs::remove(info.fifo_path);
        spdlog::info("Removed FIFO file: {}", info.fifo_path);
    } catch (const std::exception& e) {
        spdlog::warn("Failed to remove FIFO file '{}': {}", info.fifo_path, e.what());
    }
}

/**
 * @brief Run the daemon with the specified driver and control file
 *
 * @param driver The Diaspora driver instance
 * @param control_file Path to the daemon's control file
 */
void run_daemon(diaspora::Driver& driver, const std::string& control_file) {
    namespace fs = std::filesystem;

    try {
        spdlog::info("Starting Diaspora FIFO daemon...");
        spdlog::info("Driver: {}", fmt::ptr(&driver));
        spdlog::info("Control file: {}", control_file);

        // Initialize control FIFO
        int control_fd = initialize_control_fifo(control_file);
        if (control_fd == -1) {
            return;
        }

        spdlog::info("Daemon is running. Waiting for commands on control file...");
        spdlog::info("Send commands in format: <fifo-name> -> <topic-name> (key=value, ...)");
        spdlog::info("Press Ctrl+C to stop.");

        // Cached TopicHandle instances
        std::unordered_map<std::string, diaspora::TopicHandle> topics;

        // Map of FIFO paths to producer info
        std::unordered_map<std::string, ProducerInfo> producers;

        // Buffer for control file reads
        std::string control_buffer;

        // Main event loop
        while (!g_shutdown_requested) {
            // Build poll file descriptor array
            std::vector<struct pollfd> fds;
            fds.push_back({control_fd, POLLIN, 0});

            // Add all producer FIFOs
            std::vector<std::string> fifo_paths;
            for (const auto& [path, info] : producers) {
                fds.push_back({info.fd, POLLIN, 0});
                fifo_paths.push_back(path);
            }

            // Poll with 100ms timeout
            int ret = poll(fds.data(), fds.size(), 100);

            if (ret < 0) {
                if (errno == EINTR) continue;
                spdlog::error("Poll error: {}", std::strerror(errno));
                break;
            }

            if (ret == 0) continue; // Timeout

            // Check control file for new commands
            if (fds[0].revents & POLLIN) {
                char buffer[4096];
                ssize_t n = read(control_fd, buffer, sizeof(buffer));
                if (n > 0) {
                    control_buffer.append(buffer, n);

                    // Process complete lines
                    size_t pos;
                    while ((pos = control_buffer.find('\n')) != std::string::npos) {
                        std::string command = control_buffer.substr(0, pos);
                        control_buffer.erase(0, pos + 1);

                        std::string fifo_name, topic_name;
                        std::unordered_map<std::string, std::string> options;
                        if (parse_control_command(command, fifo_name, topic_name, options)) {
                            // Log the received command
                            if (options.empty()) {
                                spdlog::info("Received command: '{}' -> '{}'", fifo_name, topic_name);
                            } else {
                                std::string opts_str;
                                for (const auto& [key, value] : options) {
                                    if (!opts_str.empty()) opts_str += ", ";
                                    opts_str += key + "=" + value;
                                }
                                spdlog::info("Received command: '{}' -> '{}' ({})",
                                           fifo_name, topic_name, opts_str);
                            }

                            // Handle the command
                            handle_control_command(driver, fifo_name, topic_name, options, topics, producers);
                        } else {
                            spdlog::warn("Invalid command format: '{}'", command);
                        }
                    }
                }
            }

            // Track FIFOs that need to be closed
            std::vector<std::string> fifos_to_close;

            // Check producer FIFOs for data
            for (size_t i = 1; i < fds.size(); ++i) {
                const std::string& fifo_path = fifo_paths[i - 1];
                bool should_close = false;

                if (fds[i].revents & POLLIN) {
                    auto& info = producers[fifo_path];
                    if (!handle_producer_data(info)) {
                        should_close = true;
                    }
                }

                if (fds[i].revents & (POLLERR | POLLHUP)) {
                    spdlog::warn("POLLHUP/POLLERR on FIFO: {}", fifo_path);
                    should_close = true;
                }

                if (should_close) {
                    fifos_to_close.push_back(fifo_path);
                }
            }

            // Clean up closed FIFOs
            for (const auto& fifo_path : fifos_to_close) {
                auto it = producers.find(fifo_path);
                if (it != producers.end()) {
                    cleanup_producer(it->second);
                    producers.erase(it);
                }
            }
        }

        spdlog::info("Shutting down daemon...");

        // Cleanup: close control file
        close(control_fd);
        fs::remove(control_file);

        // Cleanup: close all producer FIFOs
        for (auto& [path, info] : producers) {
            cleanup_producer(info);
        }
        producers.clear();

    } catch (const std::exception& e) {
        spdlog::error("Error in daemon operation: {}", e.what());
        std::exit(1);
    }
}

/**
 * @brief Main entry point for the daemon
 */
int main(int argc, char** argv) {
    try {
        // Set up command-line argument parser
        TCLAP::CmdLine cmd(
            "Diaspora Stream FIFO Daemon - A daemon for managing Diaspora streaming operations",
            ' ',
            "0.4.0"
        );

        TCLAP::ValueArg<std::string> driverArg(
            "",
            "driver",
            "Name of the Diaspora driver to use (e.g., \"simple:libdiaspora-simple-backend.so\", \"mofka\")",
            true,
            "",
            "string",
            cmd
        );

        TCLAP::ValueArg<std::string> driverConfigArg(
            "",
            "driver-config",
            "Path to JSON configuration file for the driver (optional)",
            false,
            "",
            "filename",
            cmd
        );

        TCLAP::ValueArg<std::string> controlFileArg(
            "",
            "control-file",
            "Path to the daemon's control file",
            true,
            "",
            "filename",
            cmd
        );

        std::vector<std::string> allowed_levels{"trace", "debug", "info", "warn", "error", "critical", "off"};
        TCLAP::ValuesConstraint<std::string> level_constraint(allowed_levels);
        TCLAP::ValueArg<std::string> loggingArg(
            "",
            "logging",
            "Logging level",
            false,
            "info",
            &level_constraint,
            cmd
        );

        // Parse command-line arguments
        cmd.parse(argc, argv);

        // Configure spdlog
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        std::string driver_name = driverArg.getValue();
        std::string driver_config_path = driverConfigArg.getValue();
        std::string control_file = controlFileArg.getValue();

        // Load driver configuration if provided, otherwise use empty JSON object
        diaspora::Metadata driver_config;
        if (!driver_config_path.empty()) {
            driver_config = load_driver_config(driver_config_path);
        } else {
            driver_config = diaspora::Metadata{"{}"};
        }

        // Create the driver
        auto driver = create_driver(driver_name, driver_config);

        // Set up signal handlers for graceful shutdown
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        // Run the daemon
        run_daemon(driver, control_file);

        return 0;

    } catch (const TCLAP::ArgException& e) {
        spdlog::error("Error parsing arguments: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}
