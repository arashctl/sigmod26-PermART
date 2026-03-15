set(COMMON_DEFS "ALLOC_TYPE=ralloc;POOL_TYPE=none;RECLAIM_TYPE=debra;USE_RALLOC")

# set march native: 
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wextra -march=native -mavx512bw")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -gdwarf-4 -fno-omit-frame-pointer -fno-inline -Wno-unused-variable ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

add_executable(Setbench ../../microbench/main.cpp)
target_compile_definitions(Setbench PRIVATE ${COMMON_DEFS})
target_include_directories(Setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
find_library(TbbLib tbb)
find_library(NumaLib numa)
target_link_libraries(Setbench PRIVATE ${TbbLib} ${NumaLib})
target_include_directories(Setbench PRIVATE ../../common/recordmgr)

# NUMA0 dl
target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")


# NUMA1 dl
target_include_directories(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
target_link_libraries(Setbench PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")
