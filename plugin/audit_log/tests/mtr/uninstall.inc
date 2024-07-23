
disable_query_log;

SET SQL_LOG_BIN = OFF;
DROP FUNCTION audit_login_messages;
DROP DATABASE IF EXISTS sys_audit;

disable_query_log;
