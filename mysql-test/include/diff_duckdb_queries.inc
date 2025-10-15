--disable_query_log

--eval $query1_init_env

--output $MYSQL_TMP_DIR/query1
--eval $query1

--eval $query2_init_env

--output $MYSQL_TMP_DIR/query2
--eval $query2

--enable_query_log

--diff_files $MYSQL_TMP_DIR/query1 $MYSQL_TMP_DIR/query2
--echo $query1
--exec cat $MYSQL_TMP_DIR/query1
--remove_file $MYSQL_TMP_DIR/query1
--remove_file $MYSQL_TMP_DIR/query2
