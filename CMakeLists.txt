cmake_minimum_required(VERSION 3.16)
project(kvstore)

set(CMAKE_CXX_STANDARD 11)

set(LIB_SRC
        )

add_library(kvstore library.cpp)
