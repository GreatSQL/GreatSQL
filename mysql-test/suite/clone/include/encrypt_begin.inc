--echo # Prepare for creating Encrypted Table

--let $ENCRYPT_DATADIR = $MYSQL_TMP_DIR/data_encrypt
--let $ENCRYPT_ERROR_FILE = $MYSQL_TMP_DIR/boot_encrypt_log

--let BOOTSTRAP_SQL=$MYSQL_TMP_DIR/boot.sql
--let $START_PAGE_SIZE= `select @@innodb_page_size`
--let $CASE_TABLE_NAME= `select @@lower_case_table_names`
--echo # create bootstrap file

write_file $BOOTSTRAP_SQL;
CREATE DATABASE test DEFAULT CHARACTER SET latin1;
EOF

--mkdir $ENCRYPT_DATADIR

--echo # Create and start mysqld with keyring plugin.
--source include/shutdown_mysqld.inc

--replace_result $MYSQLD MYSQLD $BOOTSTRAP_SQL BOOTSTRAP_SQL $ENCRYPT_DATADIR ENCRYPT_DATADIR $START_PAGE_SIZE PAGE_SIZE $ENCRYPT_ERROR_FILE ENCRYPT_ERROR_FILE $CASE_TABLE_NAME CASE_TABLE_NAME
--let NEW_CMD = $MYSQLD --no-defaults --innodb_dedicated_server=OFF --initialize-insecure --init-file=$BOOTSTRAP_SQL --innodb_page_size=$START_PAGE_SIZE --lower_case_table_names=$CASE_TABLE_NAME --datadir=$ENCRYPT_DATADIR > $ENCRYPT_ERROR_FILE  2>&1

--exec $NEW_CMD

--replace_result $KEYRING_PLUGIN KEYRING_PLUGIN $MYSQL_TMP_DIR MYSQL_TMP_DIR $ENCRYPT_DATADIR ENCRYPT_DATADIR $CASE_TABLE_NAME CASE_TABLE_NAME
--let $restart_parameters=restart: --datadir=$ENCRYPT_DATADIR --lower_case_table_names=$CASE_TABLE_NAME --early-plugin-load="keyring_file=$KEYRING_PLUGIN" --keyring_file_data=$MYSQL_TMP_DIR/mysecret_keyring

--source include/start_mysqld.inc
