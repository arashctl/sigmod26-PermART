
set(COMMON_DEFS "UseSetbenchTids")

set(ALLOCATOR "DCMM" CACHE STRING "Choose Allocator: DCMM/PMDK/DRAM/RALLOC_NUMA")

function(add_roart_variation NAME DEFINITIONS)
    set(FINAL_NAME ${NAME})

    file(GLOB NVM_MGR_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/nvm_mgr/*.cpp)
    file(GLOB ART_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/ART/*.cpp)

    add_executable(${FINAL_NAME} ../../microbench/main.cpp ${NVM_MGR_SOURCES} ${ART_SOURCES})
    target_include_directories(${FINAL_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_include_directories(${FINAL_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/ART)
    target_include_directories(${FINAL_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/nvm_mgr)

    # Target link libpmem and libpmemobj and tbb:
    target_link_libraries(${FINAL_NAME} PRIVATE tbb)

    if(ALLOCATOR STREQUAL "PMDK")
        list(APPEND DEFINITIONS "ARTPMDK")
        target_link_libraries(${FINAL_NAME} PRIVATE pmem pmemobj)
    elseif(ALLOCATOR STREQUAL "DCMM")
        target_link_libraries(${FINAL_NAME} PRIVATE pmem pmemobj)
    elseif(ALLOCATOR STREQUAL "RALLOC_NUMA")
        list(APPEND DEFINITIONS "ARTRALLOC")
        # Reuse the pactree ones for n0dl and n1dl

        # NUMA0 dl
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/librallocn0dl.a")

        # NUMA1 dl
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")
        target_include_directories(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")
        target_link_libraries(${FINAL_NAME} PRIVATE "/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/librallocn1dl.a")

    elseif(ALLOCATOR STREQUAL "DRAM")
        list(APPEND DEFINITIONS "DRAM_MODE")
    endif()

    target_compile_definitions(${FINAL_NAME} PRIVATE ${COMMON_DEFS} ${DEFINITIONS})
endfunction()

# Define ROART variations
set(ALL_VARIATIONS "")

# Variation 1: Plain, out of the box
set(Variation1_flags "")
list(APPEND ALL_VARIATIONS "Variation1")

if(DEFINED ROART_VARIATION)
    message(STATUS "ROART_VARIATION is defined as " ${ROART_VARIATION})
    list(FIND ALL_VARIATIONS ${ROART_VARIATION} VARIATION_INDEX)

    if(VARIATION_INDEX EQUAL -1)
        message(FATAL_ERROR "Unknown ROART_VARIATION: ${ROART_VARIATION}. Valid options are: ${ALL_VARIATIONS}")
    endif()

    message(STATUS "Building only specified variation: ${ROART_VARIATION}")
    set(SELECTED_VARIATIONS ${ROART_VARIATION})
else()
    message(STATUS "ROART_VARIATION is not defined, building all variations")
    set(SELECTED_VARIATIONS ${ALL_VARIATIONS})
endif()

foreach(VARIATION IN LISTS SELECTED_VARIATIONS)
    message(STATUS "Building variation: ${VARIATION}")
    add_roart_variation(${VARIATION} "${${VARIATION}_flags}")
endforeach()
