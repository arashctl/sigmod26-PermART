# cmake_minimum_required(VERSION 2.8)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
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

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)



set(ALLOCATOR "LIBVMMALLOC" CACHE STRING "Allocator to use: LIBVMMALLOC or Jemalloc or RALLOC")


add_executable(Setbench ../../microbench/main.cpp)
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
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
elseif(ALLOCATOR STREQUAL "RALLOC")
    message(STATUS "Using ralloc (on both sockets)")
    target_compile_definitions(Setbench PRIVATE "USE_RALLOC")

    # NUMA0 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")

    # NUMA1 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")

endif()
target_link_libraries(Setbench PRIVATE ${TbbLib} ${CMAKE_THREAD_LIBS_INIT} atomic boost_system boost_thread)