set(COMMON_DEFS "")
if(DEFINED ADDITIONAL_DEFS)
    set(COMMON_DEFS "${COMMON_DEFS};${ADDITIONAL_DEFS}")
endif()


# set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wextra -march=native -mavx512bw")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -gdwarf-4 -fno-omit-frame-pointer -fno-inline -Wno-unused-variable ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

set(ALLOCATOR "PMDK" CACHE STRING "Choose allocator: PMDK/RALLOC")

set(ADDITIONAL_SOURCE_FILES
    "lbtree.cc"
    "mempool.cc"
    "nvm-common.cc"
    "tree.cc"
)

add_executable(Setbench ../../microbench/main.cpp ${ADDITIONAL_SOURCE_FILES})
target_compile_definitions(Setbench PRIVATE ${COMMON_DEFS})
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})


add_executable(Example ${CMAKE_CURRENT_SOURCE_DIR}/example.cpp ${ADDITIONAL_SOURCE_FILES})
target_compile_definitions(Example PRIVATE ${COMMON_DEFS})
target_include_directories(Example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

find_library(TbbLib tbb)
find_library(NumaLib numa)
target_link_libraries(Setbench PRIVATE ${TbbLib} ${NumaLib})
target_link_libraries(Example PRIVATE ${TbbLib} ${NumaLib})

if(ALLOCATOR STREQUAL "PMDK")
    find_library(PmemLib pmem)
    find_library(PmemobjLib pmemobj)
    target_compile_definitions(Setbench PRIVATE "PMEM")
    target_compile_definitions(Example PRIVATE "PMEM")
    target_link_libraries(Setbench PRIVATE ${PmemLib} ${PmemobjLib})
    target_link_libraries(Example PRIVATE ${PmemLib} ${PmemobjLib})
elseif(ALLOCATOR STREQUAL "RALLOC")
    message(STATUS "Using ralloc")
    target_compile_definitions(Setbench PRIVATE "USE_RALLOC;PMEM;")
    target_compile_definitions(Example PRIVATE "USE_RALLOC;PMEM;")
     # NUMA0 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")
     # NUMA1 dl
    target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
    target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")
    target_include_directories(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
    target_link_libraries(Example PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")


endif()
