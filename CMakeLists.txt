cmake_minimum_required(VERSION 3.16)
project(kvstore)

set(CMAKE_CXX_STANDARD 14)

set(SRC
        src/SkipList.cc
        #src/BPlusTree.cc
        #src/BPlusTreePredefined.h
        )

set(CXX_FLAGS
        -g
        -Wall
        -std=c++14
        -D_PTHREADS
        -Wno-unused-parameter
        -O3
        )

string(REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

add_library(kv ${SRC})
target_link_libraries(kv pthread)

add_executable(base_test test/base_test.cc)
target_link_libraries(base_test kv gtest)

add_executable(bptree_test test/bptree_test.cc)
target_link_libraries(bptree_test kv gtest)
