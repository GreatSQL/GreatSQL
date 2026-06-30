
--let $default_timeout= 30

--let $sleep_freq= 10
--let $sleep_time= `select 1.0 / $sleep_freq`

--let $start_to_wait=`select current_timestamp()`

--let $_slave_timeout= $slave_timeout
if (!$_slave_timeout)
{
  --let $_slave_timeout= `select $default_timeout * $sleep_freq`
}

--let $_loop_check = 1
while ($_loop_check)
{
    --let $in_time=`SELECT TIMESTAMPDIFF(SECOND, '$start_to_wait', current_timestamp())`
    if ($in_time >= $_slave_timeout) {
        --let $assert_text = Assert Wait Slave Status Timeout
        --let $assert_cond = 0
        --source include/assert.inc
    }

    --let $_slave_error_check= query_get_value("SHOW SLAVE STATUS", Last_Errno, 1)
    if ($_slave_error_check != "0") {
        --echo $_slave_error_check;
        --die ERROR IN TEST: Slave Status Error Check Failed
    }

    --let $_loop_check = 0
    --let $_slave_io_check= query_get_value("SHOW SLAVE STATUS", Slave_IO_Running, 1)
    --let $_slave_sql_check= query_get_value("SHOW SLAVE STATUS", Slave_SQL_Running, 1)
    if ($_slave_io_check != $_wait_io_status) {
        --let $_loop_check = 1
    }
    if ($_slave_sql_check != $_wait_sql_status) {
        --let $_loop_check = 1
    }
    
    if ($_loop_check) {
        --sleep $sleep_time
    }
}

