# Add benchmark executable
add_executable (diaspora-producer-benchmark diaspora-producer-benchmark.cpp)

# Link libraries
target_link_libraries (diaspora-producer-benchmark
    PRIVATE diaspora-stream-api
)
target_include_directories (diaspora-producer-benchmark PRIVATE ${TCLAP_INCLUDE_PATH})
