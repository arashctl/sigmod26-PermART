

set(COMMON_DEFS "HashLock;NoHelp;UseSetbenchTids;OrderedIndirectNodes")

# define a variable "ADDITIONAL_DEFS" that can be set from outside to add more definitions
if(DEFINED ADDITIONAL_DEFS)
    set(COMMON_DEFS "${COMMON_DEFS};${ADDITIONAL_DEFS}")
endif()


# set(COMMON_DEFS "HashLock;NoHelp;UseSetbenchTids;")

set(ALLOCATOR "LIBVMMALLOC" CACHE STRING "Choose allocator: LIBVMMALLOC/MALLOC/RALLOC/RALLOC_NUMA")

function(add_art_variation NAME DEFINITIONS)
    set(FINAL_NAME ${NAME})

    add_executable(${FINAL_NAME} ../../microbench/main.cpp)
    target_include_directories(${FINAL_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_compile_definitions(${FINAL_NAME} PRIVATE ${COMMON_DEFS} ${DEFINITIONS})
    target_compile_options(${FINAL_NAME} PRIVATE -Wno-invalid-offsetof)

    if(ALLOCATOR STREQUAL "LIBVMMALLOC")
        message(STATUS "Using libvmmalloc allocator")
        target_compile_definitions(${FINAL_NAME} PRIVATE "USE_LIBVMMALLOC")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/repos/vmemBranches/no-override/vmem/src/include")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/repos/vmemBranches/no-override/vmem/src/nondebug/libvmmalloc.so")

    elseif (ALLOCATOR STREQUAL "RALLOC")
        message(STATUS "Using ralloc (on one socket only)")
        target_compile_definitions(${FINAL_NAME} PRIVATE "USE_RALLOC")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc/test/libralloc.a")
    elseif (ALLOCATOR STREQUAL "RALLOC_NUMA")
        message(STATUS "Using ralloc (on both sockets)")
        target_compile_definitions(${FINAL_NAME} PRIVATE "USE_RALLOC")
        target_compile_definitions(${FINAL_NAME} PRIVATE "RALLOC_NUMA_AWARE")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_numa0/ralloc/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_numa0/ralloc/test/librallocnuma0.a")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_numa1/ralloc/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_numa1/ralloc/test/librallocnuma1.a")
    elseif(ALLOCATOR STREQUAL "MALLOC")
        message(STATUS "Using jemalloc allocator (on DRAM)")
        target_compile_definitions(${FINAL_NAME} PRIVATE "USE_MALLOC")
    endif()
endfunction()

# Define ART variations
set(ALL_VARIATIONS "")

# Variation 1: Non-versioned, flush nodes, flush pointers
set(Variation1_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5")
list(APPEND ALL_VARIATIONS "Variation1")

# Variation 2: Versioned, flush nodes, flush pointers
set(Variation2_flags "TrackPtrChases;USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;Versioned;HWWriteStamp")
list(APPEND ALL_VARIATIONS "Variation2")


set(Variation2NoPmemShenanigans_flags "TrackPtrChases;USE_CLWB;Versioned;HWWriteStamp;OldPlainVersionedHybrid")
list(APPEND ALL_VARIATIONS "Variation2NoPmemShenanigans")

set(Variation2NoChase_flags "DontChaseVersionChains;TrackPtrChases;USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;Versioned;HWWriteStamp")
list(APPEND ALL_VARIATIONS "Variation2NoChase")

# Variation 3: Versioned, flush nodes, flush pointers, medium leaf size also present
set(Variation3_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;Versioned;HWWriteStamp;ADD_MEDIUM_LEAF")
list(APPEND ALL_VARIATIONS "Variation3")

# Variation 4: Versioned, flush nodes, flush pointers, replace large leaves with single-log leaves
set(Variation4_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;Versioned;HWWriteStamp;ADD_LOG_LEAF;DO_FLUSH_LOG_ENTRIES;ADD_UNPERSISTENT_BIT_TO_LOG_ENTRY")
list(APPEND ALL_VARIATIONS "Variation4")


# Variation 5: Versioned, flush nodes, flush pointers, replace large leaves with double-log leaves
set(Variation5_flags "USE_CLWB;TrackPtrChases;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;LogEntrySpinPauseCount=5;Versioned;HWWriteStamp;ADD_LOG_LEAF;DOUBLE_LOG_LEAF;DO_PERSIST_LOG_ENTRIES;DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES")
list(APPEND ALL_VARIATIONS "Variation5")

set(Variation5DontStampLogEntries_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;LogEntrySpinPauseCount=5;Versioned;HWWriteStamp;ADD_LOG_LEAF;DOUBLE_LOG_LEAF;DO_PERSIST_LOG_ENTRIES;DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES;DONT_STAMP_LOGS;")
list(APPEND ALL_VARIATIONS "Variation5DontStampLogEntries")

set(Variation5NoUnpersistentBitForLogs_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;LogEntrySpinPauseCount=5;Versioned;HWWriteStamp;ADD_LOG_LEAF;DOUBLE_LOG_LEAF;DO_PERSIST_LOG_ENTRIES")
list(APPEND ALL_VARIATIONS "Variation5NoUnpersistentBitForLogs")

set(Variation5FlushTheOtherOne_flags "USE_CLWB;DO_FLUSH_PTRS;DO_FLUSH_NODES;DO_ADD_UNPERSISTENT_BIT;SpinUntilFlushed;SpinPauseCount=5;LogEntrySpinPauseCount=5;Versioned;HWWriteStamp;ADD_LOG_LEAF;DOUBLE_LOG_LEAF;DO_PERSIST_LOG_ENTRIES;DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES;FLUSH_THE_OTHER_LOG_ENTRY")
list(APPEND ALL_VARIATIONS "Variation5FlushTheOtherOne")


if(DEFINED ART_VARIATION)
    # if one variation is specified, only build that one
    list(FIND ALL_VARIATIONS ${ART_VARIATION} VARIATION_INDEX)

    if(VARIATION_INDEX EQUAL -1)
        message(FATAL_ERROR "Unknown ART_VARIATION: ${ART_VARIATION}. Valid options are: ${ALL_VARIATIONS}")
    endif()

    message(STATUS "Building only specified variation: ${ART_VARIATION}")
    set(SELECTED_VARIATIONS ${ART_VARIATION})
else()
    # otherwise build all variations
    message(STATUS "Building all variations: ${ALL_VARIATIONS}")
    set(SELECTED_VARIATIONS ${ALL_VARIATIONS})
endif()

foreach(VARIATION IN LISTS SELECTED_VARIATIONS)
    message(STATUS "Building variation: ${VARIATION}")
    add_art_variation(${VARIATION} "${${VARIATION}_flags}")
endforeach()
