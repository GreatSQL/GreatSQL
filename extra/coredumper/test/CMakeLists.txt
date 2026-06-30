
add_executable(coredumper_unittest coredumper_unittest.c)
add_test(NAME coredumper_unittest COMMAND coredumper_unittest)

target_link_libraries(coredumper_unittest coredumper)
# Needs access to internal headers
target_include_directories(coredumper_unittest PRIVATE "${COREDUMPER_SRC_DIR}/src/")

add_executable(linux_syscall_support_unittest linux_syscall_support_unittest.cc)
# Needs access to internal headers
target_include_directories(linux_syscall_support_unittest PRIVATE "${COREDUMPER_SRC_DIR}/src/")
add_test(NAME linux_syscall_support_unittest COMMAND linux_syscall_support_unittest)
