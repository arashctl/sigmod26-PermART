execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE CLWB)

if(CLWB)
    message(STATUS "Use clwb")
else()
    add_definitions(-DNO_CLWB)
    message(STATUS "Use clflush instaed of clwb")
endif()

set(COMMON_DEFS "")
if(DEFINED ADDITIONAL_DEFS)
    set(COMMON_DEFS "${COMMON_DEFS};${ADDITIONAL_DEFS}")
endif()

# set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wextra -march=native -mavx512bw")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -gdwarf-4 -fno-omit-frame-pointer -fno-inline -Wno-unused-variable ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

set(ALLOCATOR "PMDK" CACHE STRING "Choose allocator: PMDK/RALLOC")

# Bunch of additional source files that can't be added via globbing.
set(
    ADDITIONAL_SOURCE_FILES
    src/linkedList.cpp
    src/listNode.cpp
    src/pactree.cpp
    src/WorkerThread.cpp
    src/Oplog.cpp
    lib/PDL-ART/Tree.cpp
)

add_executable(Setbench ../../microbench/main.cpp ${ADDITIONAL_SOURCE_FILES})
target_compile_definitions(Setbench PRIVATE ${COMMON_DEFS})

add_executable(Example ${CMAKE_CURRENT_SOURCE_DIR}/example.cpp ${ADDITIONAL_SOURCE_FILES})
target_compile_definitions(Example PRIVATE ${COMMON_DEFS})

target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_include_directories(Example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(Example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(Example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

find_library(TbbLib tbb)
find_library(NumaLib numa)
target_link_libraries(Setbench PRIVATE ${TbbLib} ${NumaLib})
target_link_libraries(Example PRIVATE ${TbbLib} ${NumaLib})

if(ALLOCATOR STREQUAL "PMDK")
    find_library(PmemLib pmem)
    find_library(PmemobjLib pmemobj)
    target_compile_definitions(Setbench PRIVATE "USE_PMDK")
    target_compile_definitions(Example PRIVATE "USE_PMDK")
    target_link_libraries(Setbench PRIVATE ${PmemLib} ${PmemobjLib})
    target_link_libraries(Example PRIVATE ${PmemLib} ${PmemobjLib})
elseif(ALLOCATOR STREQUAL "RALLOC")
    message(STATUS "Using ralloc (on one socket only)")
    target_compile_definitions(Setbench PRIVATE "USE_RALLOC")
    target_compile_definitions(Example PRIVATE "USE_RALLOC")

    # NUMA0 sl 
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0sl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0sl/test/librallocn0sl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0sl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0sl/test/librallocn0sl.a")

    # NUMA0 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")

    # NUMA0 log
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0log/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0log/test/librallocn0log.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0log/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0log/test/librallocn0log.a")

    # NUMA1 sl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1sl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1sl/test/librallocn1sl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1sl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1sl/test/librallocn1sl.a")

    # NUMA1 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")

    # NUMA1 log
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1log/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1log/test/librallocn1log.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1log/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1log/test/librallocn1log.a")


endif()
