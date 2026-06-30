-- Copyright (c) 2026, GreatDB Software Co., Ltd.
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


set @eal_mode=@@greatdb_priv_mode;

SET @SAVED_SQL_MODE = @@SQL_MODE;
SET SQL_MODE = ORACLE;

DROP DATABASE IF EXISTS `plan_baseline_tmp`;
CREATE DATABASE `plan_baseline_tmp` ;

DELIMITER $$
CREATE OR REPLACE PROCEDURE plan_baseline_tmp.plan_baseline_deinit() AS
BEGIN
    IF @eal_mode <> 1 THEN
        uninstall plugin PLAN_BASELINE;
    END IF;
    show status like 'plan_baseline%';
END $$

CALL plan_baseline_tmp.plan_baseline_deinit()$$
DROP PROCEDURE plan_baseline_tmp.plan_baseline_deinit$$
DELIMITER ;

DROP DATABASE IF EXISTS `plan_baseline_tmp`;
SET SQL_MODE = @SAVED_SQL_MODE;
