cmake_minimum_required(VERSION 3.20)

project(SetBench)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_COMPILER gcc-11)
set(CMAKE_CXX_COMPILER g++-11)

# FOR BZTREE
# set(CMAKE_C_COMPILER /usr/bin/gcc)
# set(CMAKE_CXX_COMPILER /usr/bin/g++)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -gdwarf-4 -fno-omit-frame-pointer -fno-inline")

file(GLOB_RECURSE COMMON_DIRECTORIES LIST_DIRECTORIES true "${CMAKE_CURRENT_SOURCE_DIR}/common/*")
foreach(COMMON_DIR ${COMMON_DIRECTORIES})
    if (IS_DIRECTORY ${COMMON_DIR})
        list(APPEND INCLUDE_DIRS ${COMMON_DIR})
    endif ()
endforeach ()
list(REMOVE_DUPLICATES INCLUDE_DIRS)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common ${CMAKE_CURRENT_SOURCE_DIR}/common/recordmgr ${CMAKE_CURRENT_SOURCE_DIR}/common/nvm_tools ${CMAKE_CURRENT_SOURCE_DIR}/common/papi ${CMAKE_CURRENT_SOURCE_DIR}/common/rq)
foreach (INC_DIR ${INCLUDE_DIRS})
    # include_directories("${INC_DIR}")
endforeach ()


add_subdirectory(ds)