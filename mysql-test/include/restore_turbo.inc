--disable_query_log
if ($saved_gdb_enable_turbo) {
--error 0, ER_UNKNOWN_SYSTEM_VARIABLE
eval set session turbo_enable=$saved_gdb_enable_turbo;
}
--enable_query_log

