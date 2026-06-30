cmake_minimum_required(VERSION  3.5.0)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif(POLICY CMP0048)

project(kmip C CXX)

set(KMIP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(libkmip/src)
add_subdirectory(kmippp)

