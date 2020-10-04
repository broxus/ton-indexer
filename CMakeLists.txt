cmake_minimum_required(VERSION 3.10)

set(_PROJECT_NAME          ton-indexer)
set(_PROJECT_LANGUAGE      CXX)

set(_PROJECT_MAJOR_VERSION 0)
set(_PROJECT_MINOR_VERSION 0)
set(_PROJECT_PATCH_VERSION 1)

set(SUBPROJECT_LIST
        "src/ton-indexer"
        )

# Cmake module path
set(PROJECT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_ROOT_DIR}/cmake/modules")

set(_PROJECT_VERSION
        ${_PROJECT_MAJOR_VERSION}.${_PROJECT_MINOR_VERSION}.${_PROJECT_PATCH_VERSION})

project(${_PROJECT_NAME} LANGUAGES ${_PROJECT_LANGUAGE} VERSION ${_PROJECT_VERSION})

# Extlibs
set(WITH_GFLAGS OFF)
add_subdirectory(${PROJECT_ROOT_DIR}/extlibs/ton EXCLUDE_FROM_ALL)
add_subdirectory(${PROJECT_ROOT_DIR}/extlibs/libpqxx EXCLUDE_FROM_ALL)

foreach(SUBPROJ ${SUBPROJECT_LIST})
    add_subdirectory(${SUBPROJ})
endforeach()
