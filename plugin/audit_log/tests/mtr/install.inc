
disable_query_log;

SET SQL_LOG_BIN = OFF;

CREATE DATABASE IF NOT EXISTS sys_audit;

CREATE TABLE IF NOT EXISTS sys_audit.audit_log (
  name VARCHAR(64) NOT NULL,
  record VARCHAR(64) NOT NULL,
  timestamp VARCHAR(64) NOT NULL,
  command_class VARCHAR(64) NOT NULL,
  connection_id BIGINT NOT NULL,
  status VARCHAR(64) NOT NULL,
  sqltext MEDIUMTEXT NOT NULL,
  user VARCHAR(64) NOT NULL,
  host VARCHAR(64) NOT NULL,
  priv_user VARCHAR(64) NOT NULL,
  os_user VARCHAR(64) NOT NULL,
  ip VARCHAR(64) NOT NULL,
  db VARCHAR(64) NOT NULL,
  server_version VARCHAR(64),
  os_version VARCHAR(64) NOT NULL,
  server_id BIGINT NOT NULL,
  timegmt BIGINT NOT NULL,
  PRIMARY KEY record(record)
)CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ENGINE=INNODB;

CREATE FUNCTION audit_login_messages RETURNS STRING SONAME 'audit_login_messages.so';

enable_query_log;
