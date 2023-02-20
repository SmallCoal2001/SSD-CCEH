cmake_minimum_required(VERSION 3.16)
project(SSD_CCEH)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-g")

add_executable(SSD_CCEH src/hash.h src/util.h src/CCEH.h src/CCEH.cpp src/test.cpp)

target_link_libraries(SSD_CCEH pthread)