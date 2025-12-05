file (GLOB diaspora-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/*Test.cpp)

# Check if Thallium is available for ArgobotsThreadPool tests
find_package (thallium QUIET)

foreach (test-source ${diaspora-test-sources})
    get_filename_component (name ${test-source} NAME_WE)

    # Skip ArgobotsThreadPoolTest if Thallium is not available
    if (name STREQUAL "ArgobotsThreadPoolTest" AND NOT thallium_FOUND)
        message (STATUS "Skipping ${name} - Thallium not found")
        continue ()
    endif ()

    add_executable (Diaspora${name} ${test-source})
    target_link_libraries (Diaspora${name}
        PRIVATE Catch2::Catch2WithMain diaspora-stream-api coverage_config warnings_config)

    # Add Thallium for ArgobotsThreadPoolTest
    if (name STREQUAL "ArgobotsThreadPoolTest")
        target_link_libraries (Diaspora${name} PRIVATE thallium)
        target_compile_definitions (Diaspora${name} PRIVATE ENABLE_ARGOBOTS_TESTS)
    endif ()

    add_test (NAME Diaspora${name} COMMAND ./Diaspora${name})
    set_property (TEST Diaspora${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}:$ENV{LD_LIBRARY_PATH}")
    install (TARGETS Diaspora${name}
             DESTINATION bin)
endforeach ()

install (FILES diaspora-run-tests.sh
         DESTINATION bin PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE)

if (ENABLE_PYTHON)

    add_subdirectory (python)

    set (PYTHON_MODULE_DIR ${CMAKE_SOURCE_DIR}/python)
    file (GLOB_RECURSE PYTHON_DIASPORA_STREAM_TEST_FILES "${PYTHON_MODULE_DIR}/test_diaspora_*.py")

    foreach (PYTHON_TEST_FILE ${PYTHON_DIASPORA_STREAM_TEST_FILES})
        file (RELATIVE_PATH PYTHON_TEST_FILE_REL ${PYTHON_MODULE_DIR} ${PYTHON_TEST_FILE})
        string (REPLACE ".py" "" PYTHON_TEST_FILE_BASE ${PYTHON_TEST_FILE_REL})
        string (REPLACE "/" "." PYTHON_TEST_NAME ${PYTHON_TEST_FILE_BASE})
        if (${ENABLE_COVERAGE})
            message (STATUS "${PYTHON_TEST_NAME} test will run with code coverage")
            add_test (NAME ${PYTHON_TEST_NAME} COMMAND coverage run -a -m unittest ${PYTHON_TEST_NAME})
        else ()
            add_test (NAME ${PYTHON_TEST_NAME} COMMAND python -m unittest ${PYTHON_TEST_NAME})
        endif ()
        set_property (TEST ${PYTHON_TEST_NAME} PROPERTY ENVIRONMENT
                      PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
    endforeach ()

endif ()
