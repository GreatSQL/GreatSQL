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

CREATE DATABASE greatdb_plan_baseline DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE `greatdb_plan_baseline`.`gdb_sql_plan_baseline_table_info` (
  `id` int NOT NULL AUTO_INCREMENT,
   `hist_sql_plan` varchar(50) DEFAULT NULL,
  `sql_plan_baselines` varchar(50) DEFAULT NULL,
  `digest_sql_info` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `greatdb_plan_baseline`.`plan_compare_result` (
  `db_name` varchar(64) DEFAULT NULL,
  `digest_hash` varchar(64) DEFAULT NULL,
  `digest_text` longtext,
  `tb_name1` varchar(64) DEFAULT NULL,
  `id1` bigint DEFAULT NULL,
  `plan_name1` varchar(64) DEFAULT NULL,
  `cost1` decimal(7,2) DEFAULT NULL,
  `tb_name2` varchar(64) DEFAULT NULL,
  `id2` bigint DEFAULT NULL,
  `plan_name2` varchar(64) DEFAULT NULL,
  `cost2` decimal(7,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET @SAVED_SQL_MODE = @@SQL_MODE;
SET SQL_MODE = ORACLE;

DELIMITER $$
CREATE OR REPLACE PROCEDURE greatdb_plan_baseline.plan_compare_result(id1 varchar(15), id2 varchar(15)) as
sql_plan_baselines_name1 varchar(100);
hist_sql_plan_name1 varchar(100);
digest_sql_info_name1 varchar(100);
sql_plan_baselines_name2 varchar(100);
hist_sql_plan_name2 varchar(100);
digest_sql_info_name2 varchar(100);
sql_str varchar(2000);
type tklist is table of varchar(50) index by BINARY_INTEGER;
name_varray1 tklist;
name_varray2 tklist;
cc sys_refcursor;
sql_plan_baselines_res greatdb_plan_baseline.plan_compare_result%rowtype;
no_result EXCEPTION;
begin
select sql_plan_baselines,hist_sql_plan,digest_sql_info into
  sql_plan_baselines_name1,hist_sql_plan_name1,digest_sql_info_name1
  from greatdb_plan_baseline.gdb_sql_plan_baseline_table_info
    where hist_sql_plan like '%' || id1;
select sql_plan_baselines,hist_sql_plan,digest_sql_info into
  sql_plan_baselines_name2,hist_sql_plan_name2,digest_sql_info_name2
  from greatdb_plan_baseline.gdb_sql_plan_baseline_table_info
    where hist_sql_plan like '%' || id1;
name_varray1 := tklist(1=>hist_sql_plan_name1, 2=>sql_plan_baselines_name1, 3=>digest_sql_info_name1);
name_varray2 := tklist(1=>hist_sql_plan_name2, 2=>sql_plan_baselines_name2, 3=>digest_sql_info_name2);
truncate TABLE `greatdb_plan_baseline`.`plan_compare_result`;

sql_str := 'select  t1.db_name as db_name,
t1.digest_hash as digest_hash, t1.digest_text as digest_text, 
\'' || name_varray1(2) || '\' as tb_name1,t1.id as id1,
t1.plan_name as plan_name1, t1.cost as cost1,
\'' || name_varray2(2) || '\' as tb_name2, t2.id as id2, t2.plan_name as plan_name2, t2.cost as cost2 
 from greatdb_plan_baseline.' || name_varray1(2) || ' t1 join greatdb_plan_baseline.'
|| name_varray2(2) || ' t2 on t1.db_name = t2.db_name and t1.digest_hash = t2.digest_hash
 where t1.plan_name != t2.plan_name and t2.plan_name is not null'; 
open cc for sql_str;
loop
   fetch cc into sql_plan_baselines_res;
   exit when cc%notfound;
   insert into `greatdb_plan_baseline`.`plan_compare_result` values (
   sql_plan_baselines_res.db_name,sql_plan_baselines_res.digest_hash,
   sql_plan_baselines_res.digest_text, sql_plan_baselines_res.tb_name1,
   sql_plan_baselines_res.id1,
   sql_plan_baselines_res.plan_name1,sql_plan_baselines_res.cost1,
   sql_plan_baselines_res.tb_name2,
   sql_plan_baselines_res.id2,sql_plan_baselines_res.plan_name2,
   sql_plan_baselines_res.cost2);
	 select sql_plan_baselines_res;
end loop;

EXCEPTION
   WHEN no_result THEN 
      select 'empty result!';
end $$
DELIMITER ;

SET SQL_MODE = @SAVED_SQL_MODE;
