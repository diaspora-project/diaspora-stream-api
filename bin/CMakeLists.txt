# Diaspora FIFO Daemon executable

# Create the executable
add_executable (diaspora-fifo diaspora-fifo.cpp)

# Link against the diaspora-stream-api library and dependencies
target_link_libraries (diaspora-fifo
    diaspora-stream-api
    spdlog::spdlog
)

# Include directories
target_include_directories (diaspora-fifo
    PRIVATE ${TCLAP_INCLUDE_PATH}
)

# Apply coverage and warnings configurations
target_link_libraries (diaspora-fifo
    coverage_config
    warnings_config
)

# Install the executable
install (TARGETS diaspora-fifo
         RUNTIME DESTINATION bin)
