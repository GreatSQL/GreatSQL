-- Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License, version 2.0,
-- as published by the Free Software Foundation.
--
-- This program is also distributed with certain software (including
-- but not limited to OpenSSL) that is licensed under separate terms,
-- as designated in a particular file or component or in included license
-- documentation.  The authors of MySQL hereby grant you an additional
-- permission to link the program and your derivative works with the
-- separately licensed software that they have included with MySQL.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License, version 2.0, for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

--
-- The upgrade table fix from 5.0.5.1 to 5.0.5.2
--

INSERT IGNORE INTO mysql.user VALUES ('localhost','greatdb.sys','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'caching_sha2_password','$A$005$THISISACOMBINATIONOFINVALIDSALTANDPASSWORDTHATMUSTNEVERBRBEUSED','N',CURRENT_TIMESTAMP,NULL,'Y', 'Y', 'Y', NULL, NULL, NULL, NULL);

INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'APPLICATION_PASSWORD_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'AUDIT_ABORT_EXEMPT', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'AUDIT_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'AUTHENTICATION_POLICY_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'BACKUP_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'BINLOG_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'BINLOG_ENCRYPTION_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'CLONE_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'CONNECTION_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'ENCRYPTION_KEY_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'FIREWALL_EXEMPT', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'FLUSH_OPTIMIZER_COSTS', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'FLUSH_STATUS', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'FLUSH_TABLES', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'FLUSH_USER_RESOURCES', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'GROUP_REPLICATION_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'GROUP_REPLICATION_STREAM', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'INNODB_REDO_LOG_ARCHIVE', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'INNODB_REDO_LOG_ENABLE', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'PASSWORDLESS_USER_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'PERSIST_RO_VARIABLES_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'REPLICATION_APPLIER', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'REPLICATION_SLAVE_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'RESOURCE_GROUP_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'RESOURCE_GROUP_USER', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'ROLE_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SENSITIVE_VARIABLES_OBSERVER', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SERVICE_CONNECTION_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SESSION_VARIABLES_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SET_USER_ID', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SHOW_ROUTINE', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SYSTEM_USER', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'SYSTEM_VARIABLES_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'TABLE_ENCRYPTION_ADMIN', 'Y');
INSERT IGNORE INTO mysql.global_grants VALUES ('greatdb.sys', 'localhost', 'XA_RECOVER_ADMIN', 'Y');

SET @old_sql_mode = @@session.sql_mode, @@session.sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';


CREATE TABLE IF NOT EXISTS `mysql`.`greatdb_sequences`
(
  `db`         VARCHAR(64)      NOT NULL,
  `name`       VARCHAR(64)      NOT NULL,
  `start_with` BIGINT UNSIGNED  NOT NULL,
  `minvalue`   BIGINT UNSIGNED  NOT NULL,
  `maxvalue`   BIGINT UNSIGNED  NOT NULL,
  `increment`  INT              NOT NULL,
  `cycle_flag` BOOLEAN          NOT NULL,
  `cache_num`  INT UNSIGNED     NOT NULL,
  `order_flag` BOOLEAN          NOT NULL,
  PRIMARY KEY(`db`, `name`)
)
ENGINE=InnoDB STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COMMENT='greatdb sequence metadata';

CREATE TABLE IF NOT EXISTS `mysql`.`greatdb_sequences_persist`
(
  `db`        VARCHAR(64)     NOT NULL,
  `name`      VARCHAR(64)     NOT NULL,
  `currval`   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY(`db`, `name`)
)
ENGINE=InnoDB STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COMMENT='greatdb sequence persist user data';

CREATE TABLE IF NOT EXISTS `mysql`.`clone_history`
(
  `ID` int auto_increment PRIMARY KEY,
  `PID` int DEFAULT 0,
  `CLONE_TYPE` varchar(50) DEFAULT NULL,
  `STATE` char(16) DEFAULT NULL,
  `BEGIN_TIME` timestamp(3) DEFAULT NULL,
  `END_TIME` timestamp(3) DEFAULT NULL,
  `SOURCE` varchar(512) DEFAULT NULL,
  `DESTINATION` varchar(512) DEFAULT NULL,
  `ERROR_NO` int DEFAULT NULL,
  `ERROR_MESSAGE` varchar(512) DEFAULT NULL,
  `BINLOG_FILE` varchar(512) DEFAULT NULL,
  `BINLOG_POSITION` bigint DEFAULT NULL,
  `GTID_EXECUTED` varchar(4096) DEFAULT NULL,
  `START_LSN` bigint DEFAULT NULL,
  `PAGE_TRACK_LSN` bigint DEFAULT NULL,
  `END_LSN` bigint DEFAULT NULL
) ENGINE=INNODB STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COMMENT='Clone history';

CREATE TABLE IF NOT EXISTS `mysql`.`db_object_synonyms`
(
  `schema`            VARCHAR(64) NOT NULL,
  `name`              VARCHAR(64) NOT NULL,
  `target_type`       VARCHAR(64) NOT NULL,
  `target_schema`     VARCHAR(64) NOT NULL,
  `target_name`       VARCHAR(64) NOT NULL,
  `db_link`           VARCHAR(256) NOT NULL,
  PRIMARY KEY(`schema`, `name`)
)
ENGINE=InnoDB STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COMMENT='db object synonyms';


COMMIT;

-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
