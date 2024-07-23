if (!$target_sched_affinity_group_number) {
  --skip should specify target_sched_affinity_group_number
}

if (!$target_sched_affinity_group_capacity) {
  --skip should specify target_sched_affinity_group_capacity
}

--let $sched_affinity_group_number = `SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Sched_affinity_group_number'`
if ($sched_affinity_group_number < $target_sched_affinity_group_number) {
  --skip $sched_affinity_group_number less than target_sched_affinity_group_number $target_sched_affinity_group_number
}

--let $sched_affinity_group_capacity = `SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Sched_affinity_group_capacity'`
if ($sched_affinity_group_capacity != $target_sched_affinity_group_capacity) {
  --skip $sched_affinity_group_capacity not equal target_sched_affinity_group_capacity $target_sched_affinity_group_capacity
}

--echo # default config
--source include/query_sched_affinity_sys_var.inc

--echo # set foreground sys_var
--let $restart_parameters="restart: --sched_affinity_foreground_thread=0-3,5,6"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--echo # set log_writer sys_var
--let $restart_parameters="restart: --sched_affinity_log_writer=4"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--echo # set foreground sys_var and log_writer sys_var
--let $restart_parameters="restart: --sched_affinity_foreground_thread=0-3,5,6 --sched_affinity_log_writer=4"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--echo # set foreground sys_var, log_writer sys_var and numa_aware sys_var
--let $restart_parameters="restart: --sched_affinity_foreground_thread=0-3,5,6 --sched_affinity_log_writer=4 --sched_affinity_numa_aware=ON"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--echo # test foreground thread group
--let $group1=0,1,2
--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--let $restart_parameters="restart: --sched_affinity_foreground_thread=$group1,$group2_from-$group2_to"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--let $i=0
--let $connection_number = 100
while ($i<$connection_number)
{
  --connect(conn$i,localhost,root,,)
  --connection default
  SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';
  --inc $i
}

--let $i=0
while ($i<$connection_number)
{
  --disconnect conn$i
  --connection default
  SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';
  --inc $i
}

--echo # test foreground thread group with numa_aware
--let $group1=0,1,2
--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--let $restart_parameters="restart: --sched_affinity_foreground_thread=$group1,$group2_from-$group2_to --sched_affinity_numa_aware=ON"
--source include/restart_mysqld.inc
--source include/query_sched_affinity_sys_var.inc

--let $i=0
--let $connection_number = 100
while ($i<$connection_number)
{
  --connect(conn$i,localhost,root,,)
  --connection default
  SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';
  --inc $i
}

--let $i=0
while ($i<$connection_number)
{
  --disconnect conn$i
  --connection default
  SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';
  --inc $i
}

--echo # Change sched_affinity related system variables

--let $group1=0,1,2
--eval SET GLOBAL sched_affinity_foreground_thread='$group1';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_foreground_thread="";
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--let $group1=0,1,2
--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group1,$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--error ER_INVALID_CPU_STRING
--eval SET GLOBAL sched_affinity_foreground_thread='???';

--let $i=0
--let $connection_number = 100
while ($i<$connection_number)
{
  --connect(conn$i,localhost,root,,)
  --connection default
  --inc $i
}
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--error ER_INVALID_CPU_STRING
--eval SET GLOBAL sched_affinity_foreground_thread='???';

--error ER_INVALID_CPU_STRING
--eval SET GLOBAL sched_affinity_foreground_thread='1-100000';

--eval SET GLOBAL sched_affinity_foreground_thread='';
--eval SET GLOBAL sched_affinity_foreground_thread='';

--let $group1=0,1,2
--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group1,$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--let $group1=0
--let $range=6
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group1,$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_foreground_thread="";
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=0;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=1;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=0;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--let $group1=0,1,2
--let $range=1
--let $group2_from=$sched_affinity_group_capacity
--expr $group2_to=$sched_affinity_group_capacity + $range
--eval SET GLOBAL sched_affinity_foreground_thread='$group1,$group2_from-$group2_to';
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=0;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=1;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--eval SET GLOBAL sched_affinity_numa_aware=0;
SELECT SUBSTRING_INDEX(variable_value,';',2) from performance_schema.global_status WHERE variable_name='Sched_affinity_status';

--echo # Clean up
--let $i=0
while ($i<$connection_number)
{
  --disconnect conn$i
  --connection default
  --inc $i
}