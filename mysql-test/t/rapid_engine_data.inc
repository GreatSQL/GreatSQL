
--echo # Nested loop inner
let $query= select * from t1 join t2 on t1.a < t2.b and t1.b < t2.b;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # Nested loop  left 
let $query= select * from t1 left join t2 on t1.a < t2.b and t1.b < t2.b;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # Nested loop right
let $query= select * from t1 join t2 on t1.a < t2.b and t1.b < t2.b;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #Inner hash join
let $query= select * from t1 , t2 where  t1.a = t2.b;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #cross  join
let $query= select * from t1 , t2;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # hash semijoin 
let $query= select t1.* FROM t1  where t1.a in (select b from t2) ;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # RIGHT_DELIM_JOIN,DELIM_SCAN
let  $query= select t1.* FROM t1  where t1.a in (select b from t2 where t2.b < t1.b) ;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # LEFT_DELIM_JOIN ,COLUMN_DATA_SCAN
let  $query= select t1.* FROM t1  where t1.a in (select b from t2 where t2.b < t1.b) ;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # limit 
let $query= select t1.* FROM t1 limit 4;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

let $query= select t1.* FROM t1 limit 4 offset 3;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #order 

let $query= select t1.* FROM t1 order by a;
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #top N
let $query= select t1.* FROM t1  order by a desc limit 4;
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #limit PERCENT
let $query= select t1.a FROM t1  OFFSET 10 rows  FETCH FIRST 50 PERCENT ROWS ONLY;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # empty Result
let $query= select t1.* FROM t1 where a is null;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #HASH_GROUP_BY
let $query= select distinct  * from t1;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # BlockwiseNLJoin
let $query= select t1.a from t1,t2 where t1.a - t2.b between 1 and 5; 
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo # BlockwiseNLJoin  const condition 
let $query= select t1.a from t1,t2 where t1.a - t2.b between 10 and 20;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #UNGROUPED_AGGREGATE
let $query= select sum(t1.a) FROM t1;
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

--echo #window
let $query= select sum(t1.a) over() FROM t1;
--sorted_result
eval $query;
eval EXPLAIN format=tree $query;
--replace_regex /actual time=\d+.\d+/actual time=***/
eval EXPLAIN analyze $query;

