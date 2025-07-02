file (GLOB mofka-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/*Test.cpp)

foreach (test-source ${mofka-test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain mofka-api coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ./${name})
    set_property (TEST ${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}:$ENV{LD_LIBRARY_PATH}")
endforeach ()

if (ENABLE_PYTHON)

#    add_subdirectory (python)

    set (PYTHON_MODULE_DIR ${CMAKE_SOURCE_DIR}/python)
    file (GLOB_RECURSE PYTHON_MOFKA_TEST_FILES "${PYTHON_MODULE_DIR}/test_mofka_*.py")

    foreach (PYTHON_TEST_FILE ${PYTHON_MOFKA_TEST_FILES})
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
