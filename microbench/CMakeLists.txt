
# option(VERSIONING "Enable verlib versioning" ON)
# option(HYBRID "Enable verlib PPoPP'24 indirection-on-need" ON)
# option(INDIRECT "Enable verlib PPoPP'21 indirection" OFF)
# option(HWWRITESTAMP "Enable HWWriteStamp (RDTSC)" OFF)

# message(${VERSIONING})
# message(${HYBRID})
# message(${INDIRECT})

# # if (VERSIONING)
# #     message("Versioning is defined")
# #     add_compile_definitions(Versioned)
# #     if (HYBRID)
# #         message("Hybrid versioning")
# #         add_compile_definitions(HybridPPoPP24)
# #     elseif (INDIRECT)
# #         message("Indirect versioning")
# #         add_compile_definitions(IndirectPPoPP21)
# #     endif ()

# #     if (HWWRITESTAMP)
# #         message("Adding RDTSC Flag")
# #         add_compile_definitions(HWWriteStamp)
# #     endif ()
# # else ()
# #     message("No versioning")
# # endif ()

# # Don't use lock-free locks
# # add_compile_definitions(NoHelp)

# #add_compile_definitions(VERLIB_DEBUG)

# add_compile_definitions(MAX_THREADS_POW2=512)
# add_compile_definitions(CPU_FREQ_GHZ=2.1)
# add_compile_definitions(MEMORY_STATS=if\(1\))
# add_compile_definitions(MEMORY_STATS2=if\(0\))
# add_compile_definitions(DEBRA_ORIGINAL_FREE)
# #add_compile_definitions(USE_TREE_STATS)

# add_compile_options(-fno-omit-frame-pointer -mcx16 -gdwarf -mrtm)

# link_libraries(pthread dl rt)
# link_directories("${CMAKE_CURRENT_SOURCE_DIR}/../lib")

# set(RECLAIMERS debra)
# set(POOLS none)

# # cmake -Dhas_libpapi=x ../
# if (NOT DEFINED has_libpapi)
#     execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/_check_lib.sh papi OUTPUT_VARIABLE has_libpapi OUTPUT_STRIP_TRAILING_WHITESPACE)
#     message("has_libpapi was calculated: " ${has_libpapi})
# else ()
#     message("has_libpapi was explicitly provided as " ${has_libpapi})
# endif ()

# if (NOT has_libpapi EQUAL 0)
#     message("Including libpapi flags...")
# else ()
#     message("Excluding libpapi flags...")
# endif ()


# # cmake -Dhas_libnuma=x ../
# if (NOT DEFINED has_libnuma)
#     execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/_check_lib.sh numa OUTPUT_VARIABLE has_libnuma OUTPUT_STRIP_TRAILING_WHITESPACE)
#     message("has_libnuma was calculated: " ${has_libnuma})
# else ()
#     message("has_libnuma was explicitly provided as " ${has_libnuma})
# endif ()

# if (NOT has_libnuma EQUAL 0)
#     message("Including libnuma flags..")
#     add_compile_definitions(USE_LIBNUMA)
#     link_libraries(numa)
# else ()
#     message("Excluding libnuma flags...")
# endif ()

# # cmake -Duse_asan=x ../
# if (NOT DEFINED use_asan)
#     set(use_asan 0)
#     message("use_asan was set: " ${use_asan})
# else ()
#     message("use_asan was explicitly provided as " ${use_asan})
# endif ()

# if (NOT use_asan EQUAL 0)
#     add_compile_options(-fsanitize=address -static-libasan)
#     add_link_options(-fsanitize=address)
# else ()
# endif ()

# # cmake -Duse_asserts=x ../
# if (NOT DEFINED use_asserts)
#     set(use_asserts 0)
#     message("use_asserts was set: " ${use_asserts})
# else ()
#     message("use_asserts was explicitly provided as " ${use_asserts})
# endif ()

# if (use_asserts EQUAL 0)
#     add_compile_definitions(NDEBUG)
# else ()
# endif ()

# # cmake -Duse_fopenmp=x ../
# if (NOT DEFINED use_fopenmp)
#     set(use_fopenmp 1)
#     message("use_fopenmp was set: " ${use_fopenmp})
# else ()
#     message("use_fopenmp was explicitly provided as " ${use_fopenmp})
# endif ()

# if (use_fopenmp EQUAL 1)
#     find_package(OpenMP REQUIRED)
#     if (OpenMP_CXX_FOUND)
#         link_libraries(OpenMP::OpenMP_CXX)
#     endif ()
# else ()
# endif ()

# # cmake -Duse_timelines=x ../
# if (NOT DEFINED use_timelines)
#     set(use_timelines 0)
#     message("use_timelines was set: " ${use_timelines})
# else ()
#     message("use_timelines was explicitly provided as " ${use_timelines})
# endif ()

# if (use_timelines EQUAL 1)
#     add_compile_definitions(MEASURE_TIMELINE_STATS)
# else ()
# endif ()

# # cmake -Dno_optimize=x ../
# if (NOT DEFINED no_optimize)
#     set(no_optimize 0)
#     message("no_optimize was set: " ${no_optimize})
# else ()
#     message("no_optimize was explicitly provided as " ${no_optimize})
# endif ()

# if (no_optimize EQUAL 1)
#     add_compile_options(-O0 -g -fno-inline-functions -fno-inline)
# else ()
#     add_compile_options(-O3)
# endif ()


# # cmake -Duse_asserts=x ../
# if (NOT DEFINED skip_validation)
#     set(skip_validation 0)
#     message("skip_validation was set: " ${skip_validation})
# else ()
#     message("skip_validation was explicitly provided as " ${skip_validation})
# endif ()

# if (skip_validation EQUAL 1)
#     add_compile_definitions(SKIP_VALIDATION)
# else ()
# endif ()


# # Old Setbench
# # foreach (RECLAIMER ${RECLAIMERS})
# #     foreach (POOL ${POOLS})
# #         foreach (ABSOLUTE_DS ${ABSOLUTE_DATA_STRUCTURES})
# #             file(RELATIVE_PATH RELATIVE_DS "${CMAKE_CURRENT_SOURCE_DIR}/../ds/" ${ABSOLUTE_DS})
# #             add_executable(${RELATIVE_DS} main.cpp)
# #             set_target_properties(${RELATIVE_DS} PROPERTIES OUTPUT_NAME "${RELATIVE_DS}.${RECLAIMER}.p${POOL}")
# #             target_compile_definitions(${RELATIVE_DS} PRIVATE DS_TYPENAME=${RELATIVE_DS} RECLAIM_TYPE=${RECLAIMER} POOL_TYPE=${POOL} ALLOC_TYPE=new)
# #             target_include_directories(${RELATIVE_DS} PRIVATE ${ABSOLUTE_DS})
# #         endforeach ()
# #     endforeach ()
# # endforeach ()


# # My new targets with custom flags for each 

# # 1. Viper
# # file(RELATIVE_PATH VIPER_RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/../ds/" ${CMAKE_CURRENT_SOURCE_DIR}/../ds/05-viper)
# # add_executable(viper main.cpp)
# # set_target_properties(viper PROPERTIES OUTPUT_NAME "05-viper.debra.pnone")
# # target_compile_definitions(viper PRIVATE DS_TYPENAME=05-viper RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_include_directories(viper PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/05-viper)
# # target_compile_options(viper PRIVATE -march=native)
# # target_compile_definitions(viper PRIVATE BoycottParlay ParlayDistributions UseSetbenchTids PregenerateOpTypes)

# # # 2. Viper without reclamation: 
# # add_executable(viper-noreclaim main.cpp)
# # set_target_properties(viper-noreclaim PROPERTIES OUTPUT_NAME "05-viper-noreclaim.debra.pnone")
# # target_compile_definitions(viper-noreclaim PRIVATE DS_TYPENAME=05-viper RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_include_directories(viper-noreclaim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/05-viper)
# # target_compile_options(viper-noreclaim PRIVATE -march=native)
# # target_compile_definitions(viper-noreclaim PRIVATE BoycottParlay ParlayDistributions UseSetbenchTids PregenerateOpTypes VIPER_NO_RECLAMATION)


# # # 3. My PMEM Verlib ART with VMMalloc 
# # add_executable(pmem-vmmalloc main.cpp)
# # set_target_properties(pmem-vmmalloc PROPERTIES OUTPUT_NAME "04-artifact-pmem-vmmalloc.debra.pnone")
# # target_include_directories(pmem-vmmalloc PRIVATE /home/mkhalaji/repos/vmem/src/include)
# # target_link_libraries(pmem-vmmalloc PRIVATE /home/mkhalaji/repos/vmem/src/nondebug/no-override/libvmmalloc.so)
# # target_compile_definitions(pmem-vmmalloc PRIVATE DS_TYPENAME=04-artifact RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_include_directories(pmem-vmmalloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/04-artifact)
# # target_compile_options(pmem-vmmalloc PRIVATE -march=native)
# # target_compile_definitions(pmem-vmmalloc PRIVATE USE_LIBVMMALLOC Versioned HWWriteStamp USE_CLWB DO_FLUSH_PTRS DO_FLUSH_NODES O_ADD_UNPERSISTENT_BIT SpinUntilFlushed SpinPauseCount=5  AlignIndirect AlignFull OrderedIndirectNodes UseSetbenchTids NoHelp ParlayDistributions BoycottParlay PregenerateOpTypes HashLock)
# # target_compile_definitions(pmem-vmmalloc PRIVATE USE_TREE_STATS)
# # target_compile_options(pmem-vmmalloc PRIVATE -O0 -fno-omit-frame-pointer)


# # # 4. DRAM Verlib ART with MALLOC 
# # add_executable(dram-malloc main.cpp)
# # set_target_properties(dram-malloc PROPERTIES OUTPUT_NAME "04-artifact-dram-malloc.debra.pnone")
# # target_compile_definitions(dram-malloc PRIVATE DS_TYPENAME=04-artifact RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_compile_definitions(dram-malloc PRIVATE Versioned HWWriteStamp AlignIndirect AlignFull OrderedIndirectNodes UseSetbenchTids USE_MALLOC NoHelp ParlayDistributions BoycottParlay PregenerateOpTypes HashLock)
# # target_include_directories(dram-malloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/04-artifact)
# # target_compile_options(dram-malloc PRIVATE -march=native)
# # target_compile_definitions(dram-malloc PRIVATE USE_TREE_STATS)
# # # target_compile_options(dram-malloc PRIVATE -O0 -fno-omit-frame-pointer)


# # # 5. vmmalloc P-ART without versioning 
# # add_executable(p-art-vmmalloc main.cpp)
# # set_target_properties(p-art-vmmalloc PROPERTIES OUTPUT_NAME "06-p-art-vmmalloc.debra.pnone")
# # target_link_libraries(p-art-vmmalloc PRIVATE /lib/x86_64-linux-gnu/libtbb.so)
# # target_include_directories(p-art-vmmalloc PRIVATE /home/mkhalaji/repos/vmemBranches/master/vmem/src/include)
# # target_link_libraries(p-art-vmmalloc PRIVATE /home/mkhalaji/repos/vmemBranches/master/vmem/src/nondebug/libvmmalloc.so)
# # target_compile_definitions(p-art-vmmalloc PRIVATE RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_include_directories(p-art-vmmalloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/06-p-art)
# # target_compile_definitions(p-art-vmmalloc PRIVATE DS_TYPENAME=06-p-art ParlayDistributions BoycottParlay PregenerateOpTypes)
# # target_compile_options(p-art-vmmalloc PRIVATE -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64) 
# # add_library(Indexes ${CMAKE_CURRENT_SOURCE_DIR}/../ds/06-p-art/Tree.cpp)
# # target_link_libraries(p-art-vmmalloc PRIVATE Indexes)


# # # 6. vmmalloc P-ART example file 
# # add_executable(p-art-example p-art-example.cpp)
# # set_target_properties(p-art-example PROPERTIES OUTPUT_NAME "06-p-art-example.debra.pnone")
# # target_link_libraries(p-art-example PRIVATE /lib/x86_64-linux-gnu/libtbb.so)
# # target_include_directories(p-art-example PRIVATE /home/mkhalaji/repos/vmemBranches/master/vmem/src/include)
# # target_link_libraries(p-art-example PRIVATE /home/mkhalaji/repos/vmemBranches/master/vmem/src/nondebug/libvmmalloc.so)
# # target_compile_options(p-art-example PRIVATE -march=native)
# # target_include_directories(p-art-example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/06-p-art)
# # target_compile_options(p-art-example PRIVATE -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64) 
# # target_link_libraries(p-art-example PRIVATE Indexes)



# # fptree Library
# # FIND_LIBRARY(PMEM_LIBRARY pmem)
# # FIND_LIBRARY(PMEMOBJ_LIBRARY pmemobj)

# # add_library(FPTree SHARED ${CMAKE_CURRENT_SOURCE_DIR}/../ds/07-fptree/fptree.cpp)
# # target_link_libraries(FPTree PRIVATE /home/mkhalaji/repos/oneTBB/out/gnu_9.4_cxx11_64_release/libtbb.so)
# # target_include_directories(FPTree PRIVATE /home/mkhalaji/repos/oneTBB/include)
# # target_compile_options(FPTree PRIVATE -march=native -lpmem -lpmemobj -ljemalloc -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread)
# # # target_compile_definitions(FPTree PRIVATE PMEM)
# # target_link_libraries(FPTree PRIVATE ${PMEM_LIBRARY})
# # target_link_libraries(FPTree PRIVATE ${PMEMOBJ_LIBRARY})

# # # 7. fptree example 
# # add_executable(fptree-example fptree-example.cpp)
# # set_target_properties(fptree-example PROPERTIES OUTPUT_NAME "07-fptree-example.debra.pnone")
# # target_compile_definitions(fptree-example PRIVATE RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_link_libraries(fptree-example PRIVATE /home/mkhalaji/repos/oneTBB/out/gnu_9.4_cxx11_64_release/libtbb.so)
# # target_include_directories(fptree-example PRIVATE /home/mkhalaji/repos/oneTBB/include)
# # target_compile_options(fptree-example PRIVATE -march=native -lpmem -lpmemobj -ljemalloc -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread)
# # target_compile_definitions(fptree-example PRIVATE PMEM)
# # target_include_directories(fptree-example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/07-fptree)
# # target_link_libraries(fptree-example PRIVATE FPTree)
# # target_link_libraries(fptree-example PRIVATE ${PMEM_LIBRARY})
# # target_link_libraries(fptree-example PRIVATE ${PMEMOBJ_LIBRARY})


# # # # 8. fptree setbench 

# # add_executable(fptree-setbench main.cpp)
# # set_target_properties(fptree-setbench PROPERTIES OUTPUT_NAME "07-fptree-setbench.debra.pnone")
# # target_compile_definitions(fptree-setbench PRIVATE RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_link_libraries(fptree-setbench PRIVATE /home/mkhalaji/repos/oneTBB/out/gnu_9.4_cxx11_64_release/libtbb.so)
# # target_include_directories(fptree-setbench PRIVATE /home/mkhalaji/repos/oneTBB/include)
# # target_compile_options(fptree-setbench PRIVATE -march=native -lpmem -lpmemobj -ljemalloc -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread)
# # target_compile_definitions(fptree-setbench PRIVATE PMEM)
# # target_include_directories(fptree-setbench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/07-fptree)
# # target_link_libraries(fptree-setbench PRIVATE FPTree)
# # target_link_libraries(fptree-setbench PRIVATE ${PMEM_LIBRARY})
# # target_link_libraries(fptree-setbench PRIVATE ${PMEMOBJ_LIBRARY})
# # target_compile_definitions(fptree-setbench PRIVATE BoycottParlay ParlayDistributions UseSetbenchTids PregenerateOpTypes)


# # # 9. fptree setbench on dram 
# # add_executable(fptree-setbench-dram main.cpp)
# # set_target_properties(fptree-setbench-dram PROPERTIES OUTPUT_NAME "07-fptree-setbench-dram.debra.pnone")
# # target_compile_definitions(fptree-setbench-dram PRIVATE RECLAIM_TYPE=debra POOL_TYPE=none ALLOC_TYPE=new)
# # target_link_libraries(fptree-setbench-dram PRIVATE /home/mkhalaji/repos/oneTBB/out/gnu_9.4_cxx11_64_release/libtbb.so)
# # target_include_directories(fptree-setbench-dram PRIVATE /home/mkhalaji/repos/oneTBB/include)
# # target_compile_options(fptree-setbench-dram PRIVATE -march=native  -ljemalloc -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread)
# # target_include_directories(fptree-setbench-dram PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../ds/07-fptree)
# # target_link_libraries(fptree-setbench-dram PRIVATE FPTree)
# # target_compile_definitions(fptree-setbench-dram PRIVATE BoycottParlay ParlayDistributions UseSetbenchTids PregenerateOpTypes)
# # target_link_libraries(fptree-setbench-dram PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../lib/libjemalloc.so)
# # target_compile_definitions(fptree-setbench-dram PRIVATE USE_TREE_STATS)
