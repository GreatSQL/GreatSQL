-- Copyright (c) 2007, 2020, Oracle and/or its affiliates. All rights reserved.
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
-- The system tables and function for greatdb mac
--

set @have_innodb= (select count(engine) from information_schema.engines where engine='INNODB' and support != 'NO');
set @is_encrypted = (select ENCRYPTION from information_schema.INNODB_TABLESPACES where NAME='mysql');
-- Tables below are NOT treated as DD tables by MySQL server yet.

SET FOREIGN_KEY_CHECKS= 1;
SET default_storage_engine=InnoDB;

SET NAMES utf8mb4;
SET @old_sql_mode = @@session.sql_mode, @@session.sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

DROP DATABASE IF EXISTS `sys_mac`;
CREATE DATABASE `sys_mac`;

DELIMITER $$
DROP PROCEDURE IF EXISTS sys_mac.create_mac_tablespace$$
CREATE DEFINER='root'@'localhost' PROCEDURE sys_mac.create_mac_tablespace()
BEGIN
  DECLARE had_sys_mac_tablespace INT DEFAULT 0;
  select count(*) into had_sys_mac_tablespace from information_schema.innodb_tablespaces where NAME = 'gdb_sys_mac';
  IF had_sys_mac_tablespace = 0 THEN
    CREATE TABLESPACE gdb_sys_mac ADD DATAFILE 'sys_mac.ibd' ENGINE = InnoDB;
  END IF;
  IF @is_encrypted = 'Y' THEN
    ALTER TABLESPACE gdb_sys_mac ENCRYPTION = 'Y';
  END IF;
END $$
CALL sys_mac.create_mac_tablespace()$$
DROP PROCEDURE sys_mac.create_mac_tablespace$$
DELIMITER ;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_policy`(
  `p_id`      INT AUTO_INCREMENT PRIMARY KEY COMMENT 'POLICY_ID',  
  `p_name`    VARCHAR(128) NOT NULL COMMENT 'POLICY NAME',
  `enable`    BOOLEAN DEFAULT TRUE,
  UNIQUE KEY(`p_name`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='policy table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;


SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_level`(
  `p_id`    INT COMMENT 'POLICY ID',
  `l_num`   INT COMMENT 'LEVEL NUM',
  `l_name`  VARCHAR(128) COMMENT 'LEVEL NAME',
  PRIMARY KEY(`p_id`, `l_num`),
  UNIQUE KEY(`p_id`, `l_name`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='level table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_compartment`(
  `p_id`      INT COMMENT 'POLICY ID',
  `c_id`      INT AUTO_INCREMENT COMMENT 'COMPARTMENT NUM',
  `c_name`    VARCHAR(128) COMMENT 'COMPARTMENT NAME',   
  PRIMARY KEY(`p_id`, `c_name`),
  UNIQUE KEY(`c_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='compartment table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_group`(
  `p_id`          INT COMMENT 'POLICY ID',
  `g_id`          INT AUTO_INCREMENT COMMENT 'GROUP NUM',
  `g_name`        VARCHAR(128) NOT NULL COMMENT 'GROUP NAME',
  `parent_id`     INT COMMENT 'PARENT GORUP ID',
  PRIMARY KEY(`p_id`, `g_name`),
  UNIQUE KEY(`g_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='group table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_labels`(
  `l_id`      INT AUTO_INCREMENT COMMENT 'LABEL ID',
  `p_id`      INT COMMENT 'POLICY ID',
  `label`     VARCHAR(128) COMMENT 'LABEL',
  PRIMARY KEY(`l_id`),
  UNIQUE KEY(`p_id`, `label`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='labels table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;


SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_database_policy`(
  `db_name`    char(64) binary DEFAULT '' NOT NULL,
  `p_id`       INT NOT NULL COMMENT 'POLICY ID',
  `l_id`       INT NOT NULL COMMENT 'LABEL ID',
  PRIMARY KEY(`db_name`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='database label table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;


SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_table_policy`(
  `db_name`       char(64) binary DEFAULT '' NOT NULL,
  `table_name`    char(64) binary DEFAULT '' NOT NULL,
  `p_id`          INT NOT NULL COMMENT 'POLICY ID',
  `l_id`          INT NOT NULL COMMENT 'LABEL ID',
  PRIMARY KEY(`db_name`, `table_name`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='table level label table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_column_policy`(
  `db_name`       char(64) binary DEFAULT '' NOT NULL,
  `table_name`    char(64) binary DEFAULT '' NOT NULL,
  `column_name`   char(64) binary DEFAULT '' NOT NULL,
  `p_id`          INT NOT NULL,
  `l_id`          INT NOT NULL COMMENT 'LABEL ID',
  PRIMARY KEY(`db_name`, `table_name`, `column_name`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='column label table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_user_policy`(
  `user`                char(32) binary DEFAULT '' NOT NULL,
  `host`                char(255) CHARACTER SET ASCII DEFAULT '' NOT NULL,
  `p_id`                INT NOT NULL COMMENT 'POLICY ID',
  `read_label`          VARCHAR(600) NOT NULL COMMENT 'READ LABEL',
  `write_label`         VARCHAR(600) NOT NULL COMMENT 'WRITE LABEL',
  `def_read_label`      VARCHAR(600) NOT NULL COMMENT 'DEFUALT READ LABEL',
  `def_write_label`     VARCHAR(600) NOT NULL COMMENT 'DEFUALT WRITE LABEL',
  `def_row_label`       VARCHAR(600) NOT NULL COMMENT 'ROW LABEL',
  PRIMARY KEY(`user`, `host`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='user label table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_row_policy`(
  `db_name`         char(64) binary DEFAULT '' NOT NULL,
  `table_name`      char(64) binary DEFAULT '' NOT NULL,
  `column_name`     char(64) binary DEFAULT '' NOT NULL,
  `p_id`            INT NOT NULL COMMENT 'POLICY ID',
  `l_id`            INT NOT NULL COMMENT 'DEFAULT LABEL ID',
  `visible_option`  INT NOT NULL COMMENT 'VISIBLE OPTION',
  `enable`          BOOLEAN DEFAULT TRUE,
  PRIMARY KEY(`db_name`, `table_name`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='row label table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;


SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_user_privs`(
  `user`      char(32) binary DEFAULT '' NOT NULL,
  `host`      char(255) CHARACTER SET ASCII DEFAULT '' NOT NULL,
  `p_id`      INT NOT NULL COMMENT 'POLICY ID',
  `privs`     enum('read', 'full'),        
  PRIMARY KEY(`user`, `host`, `p_id`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='user privs table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS `sys_mac`.`mac_privileged_users`(
  `user`      char(32) binary DEFAULT '' NOT NULL,
  `host`      char(255) CHARACTER SET ASCII DEFAULT '' NOT NULL,
  PRIMARY KEY(`user`, `host`)
)TABLESPACE gdb_sys_mac STATS_PERSISTENT=0 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='privileged users table'";
SET @str = CONCAT(@cmd, " ENCRYPTION='", @is_encrypted, "'");
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;


-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
