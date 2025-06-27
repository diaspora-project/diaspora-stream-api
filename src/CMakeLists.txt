set (src-files
     Producer.cpp
     Consumer.cpp
     DataView.cpp
     DataDescriptor.cpp
     Metadata.cpp
     Serializer.cpp
     PartitionSelector.cpp
     Validator.cpp
     EventbridgeValidator.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (mofka-api-pkg "share/cmake/mofka")

# set version fo shared library
set (mofka-api-vers "${MOFKA_API_VERSION_MAJOR}.${MOFKA_API_VERSION_MINOR}")

# library
add_library (mofka-api ${src-files})
add_library (mofka::api ALIAS mofka-api)
target_link_libraries (mofka-api
    PUBLIC
    PkgConfig::uuid
    nlohmann_json::nlohmann_json
    nlohmann_json_schema_validator::validator
    PRIVATE
    coverage_config
    warnings_config)
target_include_directories (mofka-api PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mofka-api BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mofka-api BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mofka-api
    PROPERTIES VERSION ${MOFKA_API_VERSION}
    SOVERSION ${MOFKA_API_VERSION_MAJOR})


# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "mofka-api-config-version.cmake"
    VERSION ${MOFKA_API_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (mofka-api-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/mofka-api-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (MOFKA_API_PRIVATE_LIBS "-lmofka-api")
configure_file ("mofka-api.pc.in" "mofka-api.pc" @ONLY)

# configure config.h
configure_file ("Config.h.in" "Config.h" @ONLY)

install (EXPORT mofka-api-targets
         DESTINATION ${mofka-api-pkg}
         FILE "mofka-api-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mofka-api-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/mofka-api-config-version.cmake"
         DESTINATION ${mofka-api-pkg} )
install (DIRECTORY ../include/mofka
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mofka-api.pc"
         DESTINATION "lib/pkgconfig/")
