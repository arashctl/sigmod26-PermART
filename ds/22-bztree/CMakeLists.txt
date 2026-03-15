# Set backend to PMDK by default to build persistent version. This will be passed
# in directly to PMwCAS which provides persistence support.
set(PMEM_BACKEND "PMDK" CACHE STRING "Persistent memory backend type")
string(TOUPPER ${PMEM_BACKEND} PMEM_BACKEND)

# Both volatile and persistent versions are supported, by setting PMEM_BACKEND to:
# PMDK    : use PMDK for persistence
# EMU     : use simple shared memory for emulating persistent memory. This
#           should only be used for experimental and profiling purpose. No real
#           persistence is guaranteed.
# VOLATILE: turn off persistence and build a volatile version, no persistence
#           whatsoever. Equivalent to the original MwCAS operation.
#
# If persistent memory support is turned on, in the code we define both PMEM and
# the corresponding macro for the backend. Code that is agnostic to the backend
# is wrapped by PMEM; code that is specific to the backend is wrapped around by
# PMEMEMU (for using emulation) or PMDK (for using PMDK).
#
# Note: these macros definitions follow strictly those in PMwCAS and are needed
# to make sure of the inclusion of code in PMwCAS headers in included by BzTree.
if(${PMEM_BACKEND} STREQUAL "PMDK")
    add_definitions(-DPMEM)
    add_definitions(-DPMDK)
    message(STATUS "Persistence support: PMDK")
elseif(${PMEM_BACKEND} STREQUAL "EMU")
    add_definitions(-DPMEM)
    add_definitions(-DPMEMEMU)
    message(STATUS "Persistence support: emulation")
elseif(${PMEM_BACKEND} STREQUAL "VOLATILE")
    message(STATUS "Persistence support: off")
else()
    message(FATAL_ERROR "Unsupported persistent memory backend: ${PMEM_BACKEND}")
endif()

option(GOOGLE_FRAMEWORK "Use glog, gflags and gtest" OFF)
if(${GOOGLE_FRAMEWORK})
    add_definitions(-DGOOGLE_FRAMEWORK)
    message(STATUS "GOOGLE_FRAMEWORK is defined, will use glog, gflags and gtest")
else()
    message(STATUS "GOOGLE_FRAMEWORK is not defined, will not use glog, gflags and gtest")
endif()

option(BUILD_TESTS "Build test cases using gflags, glog and gtest" OFF)
if(${BUILD_TESTS})
    if(NOT ${GOOGLE_FRAMEWORK})
        message(FATAL_ERROR "BUILD_TESTS defined but GOOGLE_FRAMEWORK not defined")
    endif()
    add_definitions(-DBUILD_TESTS)
    message(STATUS "BUILD_TESTS: defined")
else()
    message(STATUS "BUILD_TESTS: not defined")
endif()

##################### PMwCAS #########################
# set(CMAKE_C_COMPILER /usr/bin/gcc-11)
# set(CMAKE_CXX_COMPILER /usr/bin/g++-11)

include(ExternalProject)
add_definitions(-DDESC_CAP=16)
set(PMWCAS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmwcas")
ExternalProject_Add(PMWCAS
    PREFIX "${PMWCAS_PREFIX}"
    GIT_REPOSITORY https://github.com/sfu-dis/pmwcas.git
    GIT_TAG master
    INSTALL_COMMAND ""
    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM}
    CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDESC_CAP=16 -DBUILD_APPS=${BUILD_TESTS} -DGOOGLE_FRAMEWORK=${GOOGLE_FRAMEWORK}
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
)

######################## glog #####################
# Google Log
include(FetchContent)
FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/google/glog.git
    GIT_TAG v0.4.0
)
FetchContent_GetProperties(glog)
if(NOT glog_POPULATED)
    FetchContent_Populate(glog)
    set(WITH_GFLAGS OFF CACHE BOOL "we don't use gflags")
    add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif()
FetchContent_MakeAvailable(glog)


add_executable(bztree_example example.cpp bztree.cc)
target_include_directories(bztree_example PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS)
target_include_directories(bztree_example PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS/src)
target_include_directories(bztree_example PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS/include)
target_link_directories(bztree_example PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS-build/)

target_link_libraries(bztree_example glog::glog pmem pmemobj pmwcas)
add_dependencies(bztree_example PMWCAS)

set(MAX_FREEZE_RETRY 1 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(bztree_example PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})
#target_compile_definitions(bztree_static PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

set(ENABLE_MERGE 0 CACHE STRING "MAX retry on frozen node")
message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
target_compile_definitions(bztree_example PRIVATE ENABLE_MERGE=${ENABLE_MERGE})


add_executable(Setbench ../../microbench/main.cpp bztree.cc)
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(Setbench PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS)
target_include_directories(Setbench PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS/src)
target_include_directories(Setbench PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS/include)
target_link_directories(Setbench PRIVATE ${PMWCAS_PREFIX}/src/PMWCAS-build/)

target_link_libraries(Setbench glog::glog pmem pmemobj pmwcas)
add_dependencies(Setbench PMWCAS)

set(MAX_FREEZE_RETRY 1 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(Setbench PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})
#target_compile_definitions(bztree_static PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

set(ENABLE_MERGE 0 CACHE STRING "MAX retry on frozen node")
message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
target_compile_definitions(Setbench PRIVATE ENABLE_MERGE=${ENABLE_MERGE})



