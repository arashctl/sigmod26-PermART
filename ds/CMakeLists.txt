# Print ds_name: 
# set(ds_name "" CACHE STRING "Name of the data structure directory to build")

message("ds_name: " ${ds_name})

# Common Settings 
add_compile_definitions(MAX_THREADS_POW2=512)
add_compile_definitions(CPU_FREQ_GHZ=2.1)
add_compile_definitions(MEMORY_STATS=if\(1\))
add_compile_definitions(MEMORY_STATS2=if\(0\))
add_compile_definitions(DEBRA_ORIGINAL_FREE)

find_library(TbbLib tbb)
link_libraries(${TbbLib})


# Only don't add -DUSE_GSTATS if explicitly disabled: 
if (NOT DEFINED use_gstats)
    set(use_gstats 1)
    message("use_gstats was set: " ${use_gstats})
else ()
    message("use_gstats was explicitly provided as " ${use_gstats})
endif ()

if (use_gstats EQUAL 1)
    add_compile_definitions(USE_GSTATS)
endif ()

# Only don't add -DUSE_TREE_STATS if explicitly disabled:
if (NOT DEFINED use_tree_stats)
    set(use_tree_stats 1)
    message("use_tree_stats was set: " ${use_tree_stats})
else ()
    message("use_tree_stats was explicitly provided as " ${use_tree_stats})
endif ()

if (use_tree_stats EQUAL 1)
    add_compile_definitions(USE_TREE_STATS)
endif ()


add_compile_options(-mcx16 -mrtm)


link_libraries(pthread dl rt)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../lib)

set(RECLAIMERS debra)
set(POOLS none)

# cmake -Dhas_libpapi=x ../
if (NOT DEFINED has_libpapi)
    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/_check_lib.sh papi OUTPUT_VARIABLE has_libpapi OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("has_libpapi was calculated: " ${has_libpapi})
else ()
    message("has_libpapi was explicitly provided as " ${has_libpapi})
endif ()

if (NOT has_libpapi EQUAL 0)
    message("Including libpapi flags...")
    add_compile_definitions(USE_PAPI)
    link_libraries(papi)
else ()
    message("Excluding libpapi flags...")
endif ()


# cmake -Dhas_libnuma=x ../
if (NOT DEFINED has_libnuma)
    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/_check_lib.sh numa OUTPUT_VARIABLE has_libnuma OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("has_libnuma was calculated: " ${has_libnuma})
else ()
    message("has_libnuma was explicitly provided as " ${has_libnuma})
endif ()

if (NOT has_libnuma EQUAL 0)
    message("Including libnuma flags..")
    add_compile_definitions(USE_LIBNUMA)
    link_libraries(numa)
else ()
    message("Excluding libnuma flags...")
endif ()

# cmake -Duse_asan=x ../
if (NOT DEFINED use_asan)
    set(use_asan 0)
    message("use_asan was set: " ${use_asan})
else ()
    message("use_asan was explicitly provided as " ${use_asan})
endif ()

if (NOT use_asan EQUAL 0)
    add_compile_options(-fsanitize=address -static-libasan)
    add_link_options(-fsanitize=address)
else ()
endif ()

# cmake -Duse_fopenmp=x ../
if (NOT DEFINED use_fopenmp)
    set(use_fopenmp 1)
    message("use_fopenmp was set: " ${use_fopenmp})
else ()
    message("use_fopenmp was explicitly provided as " ${use_fopenmp})
endif ()

if (use_fopenmp EQUAL 1)
    find_package(OpenMP REQUIRED)
    if (OpenMP_CXX_FOUND)
        link_libraries(OpenMP::OpenMP_CXX)
    endif ()
else ()
endif ()

# cmake -Duse_timelines=x ../
if (NOT DEFINED use_timelines)
    set(use_timelines 0)
    message("use_timelines was set: " ${use_timelines})
else ()
    message("use_timelines was explicitly provided as " ${use_timelines})
endif ()

if (use_timelines EQUAL 1)
    add_compile_definitions(MEASURE_TIMELINE_STATS)
else ()
endif ()



# cmake -Duse_asserts=x ../
if (NOT DEFINED skip_validation)
    set(skip_validation 0)
    message("skip_validation was set: " ${skip_validation})
else ()
    message("skip_validation was explicitly provided as " ${skip_validation})
endif ()

if (skip_validation EQUAL 1)
    add_compile_definitions(SKIP_VALIDATION)
else ()
endif ()



# Print ds_name: 
message("ds_name: " ${ds_name})
if (NOT DEFINED ds_name)
    message(FATAL_ERROR "ds_name variable not defined. Please provide it with -Dds_name=<dir_name>")
endif()

message("Building data structure: " ${ds_name})
add_subdirectory(${ds_name})