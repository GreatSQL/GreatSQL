-- source include/big_test.inc
-- source ../include/ps_truncate_all_tables.inc
# Tests for sys schema
# Verify the sys.schema_auto_increment_columns view

# Ensure structure changes don't slip in
DESC sys.schema_auto_increment_columns;

# Make sure view select does not error, but ignore results
--disable_result_log
SELECT * FROM sys.schema_auto_increment_columns;
--enable_result_log

# Test the output of the view is as expected
CREATE DATABASE auto_incs;

CREATE TABLE auto_incs.tinyintcol (
  id TINYINT AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.tinyintcol (foo) VALUES (100);
INSERT INTO auto_incs.tinyintcol (foo) (SELECT foo FROM auto_incs.tinyintcol);
INSERT INTO auto_incs.tinyintcol (foo) (SELECT foo FROM auto_incs.tinyintcol);
INSERT INTO auto_incs.tinyintcol (foo) (SELECT foo FROM auto_incs.tinyintcol);
INSERT INTO auto_incs.tinyintcol (foo) (SELECT foo FROM auto_incs.tinyintcol);

CREATE TABLE auto_incs.tinyintcolunsigned (
  id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.tinyintcolunsigned (foo) (SELECT foo FROM auto_incs.tinyintcol);

CREATE TABLE auto_incs.smallintcol (
  id SMALLINT AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.smallintcol (foo) VALUES (200);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);
INSERT INTO auto_incs.smallintcol (foo) (SELECT foo FROM auto_incs.smallintcol);

CREATE TABLE auto_incs.smallintcolunsigned (
  id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  foo INT
);


INSERT INTO auto_incs.smallintcolunsigned (foo) (SELECT foo FROM auto_incs.smallintcol);

CREATE TABLE auto_incs.mediumintcol (
  id MEDIUMINT AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.mediumintcol (foo) VALUES (300);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);
INSERT INTO auto_incs.mediumintcol (foo) (SELECT foo FROM auto_incs.mediumintcol);

CREATE TABLE auto_incs.mediumintcolunsigned (
  id MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  foo INT
);


INSERT INTO auto_incs.mediumintcolunsigned (foo) (SELECT foo FROM auto_incs.mediumintcol);

CREATE TABLE auto_incs.intcol (
  id INT AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.intcol (foo) VALUES (400);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);
INSERT INTO auto_incs.intcol (foo) (SELECT foo FROM auto_incs.intcol);

CREATE TABLE auto_incs.intcolunsigned (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.intcolunsigned (foo) (SELECT foo FROM auto_incs.intcol);

CREATE TABLE auto_incs.bigintcol (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.bigintcol (foo) VALUES (500);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);
INSERT INTO auto_incs.bigintcol (foo) (SELECT foo FROM auto_incs.bigintcol);

CREATE TABLE auto_incs.bigintcolunsigned (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  foo INT
);

INSERT INTO auto_incs.bigintcolunsigned (foo) (SELECT foo FROM auto_incs.bigintcol);

analyze table auto_incs.tinyintcol;
analyze table auto_incs.tinyintcolunsigned;
analyze table auto_incs.smallintcol;
analyze table auto_incs.smallintcolunsigned;
analyze table auto_incs.mediumintcol;
analyze table auto_incs.mediumintcolunsigned;
analyze table auto_incs.intcol;
analyze table auto_incs.intcolunsigned;
analyze table auto_incs.bigintcol;
analyze table auto_incs.bigintcolunsigned;
SELECT * FROM sys.schema_auto_increment_columns ORDER BY table_schema, table_name;

DROP DATABASE auto_incs;
