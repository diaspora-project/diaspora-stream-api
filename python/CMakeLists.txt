add_library (pymofka_api MODULE py-mofka-api.cpp)
target_link_libraries (pymofka_api PRIVATE pybind11::module mofka-api PRIVATE coverage_config)
pybind11_extension (pymofka_api)
pybind11_strip (pymofka_api)

set (PY_MOFKA_TARGETS pymofka_api)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

install (TARGETS ${PY_MOFKA_TARGETS}
         EXPORT mofka-api-targets
         ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
         LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)

#install (DIRECTORY mochi/mofka
#        DESTINATION lib/python${PY_VERSION}/site-packages/mochi)
