--source include/elide_costs.inc

--echo #
--echo # Test that histograms are used for optimizing queries that are
--echo # offloaded to a secondary storage engine.
--echo #
SET @@use_secondary_engine = ON;
--disable_query_log
eval INSTALL PLUGIN mock SONAME '$MOCK_PLUGIN';
--enable_query_log

CREATE TABLE t1(id INT PRIMARY KEY, x INT NOT NULL) SECONDARY_ENGINE MOCK;
INSERT INTO t1 VALUES
(1, 1), (2, 1), (3, 1), (4, 1), (5, 1),
(6, 1), (7, 1), (8, 1), (9, 2), (10, 2);

ALTER TABLE t1 SECONDARY_LOAD;

# Without histograms, all three queries get the same selectivity (10%).
ANALYZE TABLE t1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 2;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 3;

# With histograms, the selectivity varies with the constant used in
# the predicate.
ANALYZE TABLE t1 UPDATE HISTOGRAM ON x WITH 10 BUCKETS;
ANALYZE TABLE t1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 2;
# The hypergraph optimizer estimates 0.01 rows when the value is not
# found in the histogram, whereas the old optimizer estimates 1 row.
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 3;

# Verify that the secondary storage engine is used for the queries
# (they all return empty results when offloaded to the mock engine).
SELECT * FROM t1 WHERE x = 1;
SELECT * FROM t1 WHERE x = 2;
SELECT * FROM t1 WHERE x = 3;

# Drop the histogram and lose the histogram statistics in the query plans.
ANALYZE TABLE t1 DROP HISTOGRAM ON x;
ANALYZE TABLE t1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 2;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 3;

# Recreate the histogram and get the good selectivity estimates back.
ANALYZE TABLE t1 UPDATE HISTOGRAM ON x WITH 10 BUCKETS;
ANALYZE TABLE t1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 1;
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 2;
# The hypergraph optimizer estimates 0.01 rows when the value is not
# found in the histogram, whereas the old optimizer estimates 1 row.
--replace_regex $elide_costs
EXPLAIN FORMAT=TREE SELECT * FROM t1 WHERE x = 3;

DROP TABLE t1;

--disable_query_log
UNINSTALL PLUGIN mock;
--enable_query_log
