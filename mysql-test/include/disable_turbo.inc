--disable_query_log
--let $saved_gdb_enable_turbo=0;
--error 0, ER_UNKNOWN_SYSTEM_VARIABLE
--let $saved_gdb_enable_turbo= `select @@turbo_enable`
--error 0, ER_UNKNOWN_SYSTEM_VARIABLE
set turbo_enable=off;

--enable_query_log
