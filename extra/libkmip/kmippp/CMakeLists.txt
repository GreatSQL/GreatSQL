
add_library(
  kmippp
  STATIC
  core_error.hpp
  core_error.cpp
  kmippp.cpp
  )

target_link_libraries(kmippp kmip)
set_property(TARGET kmippp PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(
  kmippp PUBLIC
  $<BUILD_INTERFACE:${KMIP_SOURCE_DIR}/kmippp/>
  $<INSTALL_INTERFACE:include>
  )
set_target_properties(
  kmippp PROPERTIES PUBLIC_HEADER
  "kmippp.h"
  )


export(TARGETS kmip kmippp FILE "kmippp.cmake")

install(
  TARGETS kmippp
  EXPORT kmippp
  DESTINATION cmake
  ARCHIVE DESTINATION lib
  PUBLIC_HEADER DESTINATION include/
  LIBRARY DESTINATION lib)

macro(add_pp_demo name)
  add_executable(demo_pp_${name} demo_${name}.cpp)
  target_link_libraries(demo_pp_${name} kmippp)
endmacro()

add_pp_demo(create)
add_pp_demo(activate)
add_pp_demo(get)
add_pp_demo(get_name)
add_pp_demo(register)
add_pp_demo(locate)
add_pp_demo(all)
add_pp_demo(all_secrets)
add_pp_demo(revoke)
add_pp_demo(get_secret)
add_pp_demo(register_secret)
add_pp_demo(destroy)



