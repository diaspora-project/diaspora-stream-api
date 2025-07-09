pybind11_add_module (pymofka_api py-mofka-api.cpp)
target_link_libraries (pymofka_api PRIVATE mofka-api coverage_config)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

if (NOT DEFINED SKBUILD_PROJECT_NAME)
    install (TARGETS pymofka_api
             EXPORT mofka-api-targets
             ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
             LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)
    install (DIRECTORY mochi/mofka
             DESTINATION lib/python${PY_VERSION}/site-packages/mochi)
else ()
    install (TARGETS pymofka_api DESTINATION .)
    install (DIRECTORY mochi
             DESTINATION .)
endif ()
