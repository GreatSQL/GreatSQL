cmake_minimum_required(VERSION  2.8.12)
if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif(POLICY CMP0048)
project(coredumper C CXX)
# We do not wish to work without threading support
# TODO: remove related ifdefs
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(COREDUMPER_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(src)

include(CTest)
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(test)
endif()
