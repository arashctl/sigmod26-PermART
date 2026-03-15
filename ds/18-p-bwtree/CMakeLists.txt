
if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflush OUTPUT_VARIABLE ENABLE_CLFLUSH)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflushopt OUTPUT_VARIABLE ENABLE_CLFLUSHOPT)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE ENABLE_CLWB)

if(ENABLE_CLWB)
    add_definitions(-DCLWB)
    message(STATUS "Looking for clwb instruction - found")
elseif(ENABLE_CLFLUSHOPT)
    add_definitions(-DCLFLUSH_OPT)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - found")
elseif(ENABLE_CLFLUSH)
    add_definitions(-DCLFLUSH)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - not found")
    message(STATUS "Looking for clflush instruction - found")
else()
    message(FATAL_ERROR "Cannot find any flush instructions (clflush, clflushopt, clwb)")
endif()

## Basic configurations
# add_definitions(-DBWTREE_NODEBUG)

# find_library(JemallocLib jemalloc)
# find_library(TbbLib tbb)
# find_package (Threads)

# set(INDEX_FILES src/bwtree.cpp)

# add_library(Indexes ${INDEX_FILES})
# target_link_libraries(Indexes ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT})

# set(P_BW_TEST example.cpp)
# add_executable(example ${P_BW_TEST})

# target_link_libraries(example Indexes atomic boost_system boost_thread)

set(ALLOCATOR "LIBVMMALLOC" CACHE STRING "Allocator to use: LIBVMMALLOC or Jemalloc")


add_executable(Setbench ../../microbench/main.cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/bwtree.cpp)
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_definitions(Setbench PRIVATE "-DBWTREE_NODEBUG")
find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package(Threads)
if(ALLOCATOR STREQUAL "LIBVMMALLOC")
    message(STATUS "Using libvmmalloc allocator")
    # target_include_directories(Setbench PRIVATE "/home/mkhalaji/repos/vmemBranches/master/vmem/src/include")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/repos/vmemBranches/master/vmem/src/nondebug/libvmmalloc.so")
elseif(ALLOCATOR STREQUAL "Jemalloc")
    message(STATUS "Using jemalloc allocator")
    target_link_libraries(Setbench PRIVATE ${JemallocLib})
endif()
target_link_libraries(Setbench PRIVATE ${TbbLib} ${CMAKE_THREAD_LIBS_INIT} atomic boost_system boost_thread)
