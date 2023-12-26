-- Copyright (c) 2023, GreatDB Software Co., Ltd.
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



COMMIT;

-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
