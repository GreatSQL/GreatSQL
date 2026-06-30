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


--
-- The system tables of MySQL Server
--
SET NAMES utf8mb4;
SET CHARACTER_SET_CLIENT=utf8mb4;
SET COLLATION_CONNECTION=utf8mb4_0900_ai_ci;
SET @old_sql_mode = @@session.sql_mode, @@session.sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

/*define producer or function*/
DROP PROCEDURE IF EXISTS sys_audit.audit_check_all;
DELIMITER $$
CREATE PROCEDURE sys_audit.audit_check_all()
BEGIN
  SELECT a.id as id, 'not equal' as result
  FROM sys_audit.audit_log_data a, sys_audit.audit_log_digest b
  WHERE a.id = b.id
  AND SHA2(CONCAT(a.ts, a.e_class, a.e_sub, a.fields), 256) <> b.digest 
  UNION ALL SELECT a.id as id, 'no digest' as result 
  FROM sys_audit.audit_log_data a 
  WHERE a.id NOT IN ( SELECT b.id FROM sys_audit.audit_log_digest b ) 
  UNION ALL SELECT a.id as id, 'no data' as result 
  FROM sys_audit.audit_log_digest a 
  WHERE a.id NOT IN ( SELECT b.id FROM sys_audit.audit_log_data b );
END $$
DELIMITER ;

-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
