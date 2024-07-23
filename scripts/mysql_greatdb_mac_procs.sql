-- Copyright (c) 2024, GreatDB Software Co., Ltd.
-- All rights reserved.
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
DELIMITER $$

/*find the level in the label*/
DROP FUNCTION IF EXISTS `sys_mac`.`MAC_FIND_LEVEL` $$
CREATE DEFINER='root'@'localhost' FUNCTION `sys_mac`.`MAC_FIND_LEVEL`(
  `LEVEL_NUM`   INT,
  `LABEL_VALUE` VARCHAR(600))
RETURNS INT
DETERMINISTIC
BEGIN
  SET @level_val = SUBSTRING_INDEX(LABEL_VALUE,':', 1);
  IF @level_val = LEVEL_NUM THEN
    RETURN 1;
  END IF;
  RETURN 0;
END $$

/*find the commpartment in the label*/
DROP FUNCTION IF EXISTS `sys_mac`.`MAC_FIND_COMPART` $$
CREATE DEFINER='root'@'localhost' FUNCTION `sys_mac`.`MAC_FIND_COMPART`(
  `V_COMPART_ID`   INT,
  `LABEL_VALUE` VARCHAR(600))
RETURNS INT
DETERMINISTIC
BEGIN
  DECLARE compart_arry VARCHAR(600);
  DECLARE com_num INT DEFAULT 0;
  DECLARE it INT DEFAULT 0;
  DECLARE compart_id INT DEFAULT 0;
  SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(LABEL_VALUE,':', 2),':',-1) INTO compart_arry;
  IF compart_arry IS NULL OR compart_arry = '' THEN
    RETURN 0;
  END IF;
  SET it = 1;
  SET com_num = LENGTH(compart_arry) - LENGTH(REPLACE(compart_arry,',','')) + 1;
  WHILE it <= com_num DO
    SET compart_id = SUBSTRING_INDEX(SUBSTRING_INDEX(compart_arry, ',', it), ',', -1);
    IF compart_id = V_COMPART_ID THEN
      RETURN 1;
    END IF;
    SET it = it + 1;
  END WHILE;
  RETURN 0;
END $$

/*find the group in the label*/
DROP FUNCTION IF EXISTS `sys_mac`.`MAC_FIND_GROUP` $$
CREATE DEFINER='root'@'localhost' FUNCTION `sys_mac`.`MAC_FIND_GROUP`(
  `V_GROUP_ID`   INT,
  `LABEL_VALUE` VARCHAR(600))
RETURNS INT
DETERMINISTIC
BEGIN
  DECLARE group_arry VARCHAR(600);
  DECLARE group_id INT DEFAULT 0;
  DECLARE com_num INT DEFAULT 0;
  DECLARE it INT DEFAULT 0;
  SELECT SUBSTRING_INDEX(LABEL_VALUE,':',-1) INTO group_arry;
  IF group_arry IS NULL OR group_arry = '' THEN
    RETURN 0;
  END IF;
  SET it = 1;
  SET com_num = LENGTH(group_arry) - LENGTH(REPLACE(group_arry,',','')) + 1;
  WHILE it <= com_num DO
    SET group_id = SUBSTRING_INDEX(SUBSTRING_INDEX(group_arry, ',', it), ',', -1);
    IF group_id = V_GROUP_ID THEN
      RETURN 1;
    END IF;
    SET it = it + 1;
  END WHILE;
  RETURN 0;
END $$

DROP FUNCTION IF EXISTS `sys_mac`.`MAC_CHECK_LABEL_CONTINAS` $$
CREATE DEFINER='root'@'localhost' FUNCTION `sys_mac`.`MAC_CHECK_LABEL_CONTINAS`(
  `LABEL_ONE`  VARCHAR(600),
  `LABEL_TWO`   VARCHAR(600))
RETURNS INT
DETERMINISTIC
BEGIN
  DECLARE compart_arry VARCHAR(600);
  DECLARE group_arry VARCHAR(600);
  DECLARE label_state INT DEFAULT 0;
  DECLARE compart_id INT DEFAULT 0;
  DECLARE group_id INT DEFAULT 0;
  DECLARE com_num INT DEFAULT 0;
  DECLARE it INT DEFAULT 0;

  SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(LABEL_TWO,':', 2),':',-1) INTO compart_arry;
  SELECT SUBSTRING_INDEX(LABEL_TWO,':',-1) INTO group_arry;

  /*deal with the compartment*/
  IF compart_arry IS NOT NULL AND compart_arry != '' THEN
    SET it = 1;
    SET com_num = LENGTH(compart_arry) - LENGTH(REPLACE(compart_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET compart_id = SUBSTRING_INDEX(SUBSTRING_INDEX(compart_arry, ',', it), ',', -1);
      SELECT sys_mac.MAC_FIND_COMPART(compart_id, LABEL_ONE) INTO label_state;
      IF label_state = 0 THEN
        RETURN 0;
      END IF;
      SET it = it + 1;
    END WHILE;
  END IF;
  /*deal with the group*/
  IF group_arry IS NOT NULL AND group_arry != '' THEN
    SET it = 1;
    SET com_num = LENGTH(group_arry) - LENGTH(REPLACE(group_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET group_id = SUBSTRING_INDEX(SUBSTRING_INDEX(group_arry, ',', it), ',', -1);
      SELECT sys_mac.MAC_FIND_GROUP(group_id, LABEL_ONE) INTO label_state;
      IF label_state = 0 THEN
        RETURN 0;
      END IF;
      SET it = it + 1;
    END WHILE;
  END IF;
  RETURN 1;
END $$

DROP FUNCTION IF EXISTS `sys_mac`.`MAC_GET_LABEL_CONTINAS` $$
CREATE DEFINER='root'@'localhost' FUNCTION `sys_mac`.`MAC_GET_LABEL_CONTINAS`(
  `LABEL_ONE`  VARCHAR(600),
  `LABEL_TWO`   VARCHAR(600))
RETURNS VARCHAR(600)
DETERMINISTIC
BEGIN
  DECLARE compart_arry VARCHAR(600);
  DECLARE group_arry VARCHAR(600);
  DECLARE out_value VARCHAR(600);
  DECLARE label_state INT DEFAULT 0;
  DECLARE compart_id INT DEFAULT 0;
  DECLARE group_id INT DEFAULT 0;
  DECLARE com_num INT DEFAULT 0;
  DECLARE it INT DEFAULT 0;

  SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(LABEL_TWO,':', 2),':',-1) INTO compart_arry;
  SELECT SUBSTRING_INDEX(LABEL_TWO,':',-1) INTO group_arry;

  SET out_value = ':';
  /*deal with the compartment*/
  IF compart_arry IS NOT NULL AND compart_arry != '' THEN
    SET it = 1;
    SET com_num = LENGTH(compart_arry) - LENGTH(REPLACE(compart_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET compart_id = SUBSTRING_INDEX(SUBSTRING_INDEX(compart_arry, ',', it), ',', -1);
      SELECT sys_mac.MAC_FIND_COMPART(compart_id, LABEL_ONE) INTO label_state;
      IF label_state = 1 THEN
        SELECT CONCAT(out_value, compart_id, ',') INTO out_value;
      END IF;
      SET it = it + 1;
    END WHILE;
  END IF;
  
  IF (SELECT RIGHT(out_value, 1)) = ',' THEN
    SELECT LEFT(out_value, LENGTH(out_value) - 1) INTO out_value;
  END IF;

  SELECT CONCAT(out_value, ':') INTO out_value;
  
  /*deal with the group*/
  IF group_arry IS NOT NULL AND group_arry != '' THEN
    SET it = 1;
    SET com_num = LENGTH(group_arry) - LENGTH(REPLACE(group_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET group_id = SUBSTRING_INDEX(SUBSTRING_INDEX(group_arry, ',', it), ',', -1);
      SELECT sys_mac.MAC_FIND_GROUP(group_id, LABEL_ONE) INTO label_state;
      IF label_state = 1 THEN
        SELECT CONCAT(out_value, group_id, ',') INTO out_value;
      END IF;
      SET it = it + 1;
    END WHILE;
  END IF;
  IF (SELECT RIGHT(out_value, 1)) = ',' THEN
    SELECT LEFT(out_value, LENGTH(out_value) - 1) INTO out_value;
  END IF;
  RETURN out_value;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_POLICY`(
  `POLICY_NAME`   VARCHAR(128))
BEGIN
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy already exits", MYSQL_ERRNO=7700;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;

  IF POLICY_NAME = '' OR POLICY_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7700;
  END IF;
  INSERT INTO `sys_mac`.`mac_policy`(`p_id`, `p_name`) VALUES(NULL, POLICY_NAME);

  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_POLICY`(
  `POLICY_NAME`    VARCHAR(128),
  `NEW_NAME`       VARCHAR(128))
BEGIN
  DECLARE policy_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy already exits", MYSQL_ERRNO=7700;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF NEW_NAME = '' OR NEW_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7700;
  END IF;

  SELECT COUNT(*) INTO policy_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF policy_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7700;
  ELSE
    UPDATE `sys_mac`.`mac_policy` SET `p_name` = NEW_NAME WHERE `p_name` = POLICY_NAME;
  END IF;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_POLICY`(
  `POLICY_NAME` VARCHAR(128))
BEGIN
  DECLARE policy_state INT DEFAULT 0;
  DECLARE policy_id INT;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO policy_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF policy_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7700;
  END IF;

  SELECT SUM(a) INTO policy_state FROM (SELECT COUNT(*) a FROM `sys_mac`.`mac_level` WHERE `p_id` = policy_id UNION ALL
                                        SELECT COUNT(*) a FROM `sys_mac`.`mac_compartment` WHERE `p_id` = policy_id UNION ALL
                                        SELECT COUNT(*) a FROM `sys_mac`.`mac_group` WHERE `p_id` = policy_id) AS tab;
  IF policy_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy is being used, connot be deleted", MYSQL_ERRNO=7700;
  END IF;
  DELETE FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_LEVEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_LEVEL`(
  `POLICY_NAME` VARCHAR(128),
  `LEVEL_NAME`  VARCHAR(128),
  `LEVEL_NUM`   INT)
BEGIN
  DECLARE level_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name or num already exits", MYSQL_ERRNO=7701;
    ELSE
      RESIGNAL;
    END IF;
  END;
  
  START TRANSACTION;
  IF LEVEL_NAME = '' OR  LEVEL_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name should not be empty", MYSQL_ERRNO=7701;
  END IF;
  IF LEVEL_NUM < 0 or LEVEL_NUM > 9999 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level num should between 0 and 9999", MYSQL_ERRNO=7701;
  END IF;
  SELECT LOCATE(':',LEVEL_NAME) + LOCATE(',',LEVEL_NAME) INTO level_state;
  IF level_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name should not contains ; or ,", MYSQL_ERRNO=7701;
  END IF;

  SELECT COUNT(*) INTO level_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF level_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7701;
  END IF;
  INSERT INTO `sys_mac`.`mac_level` SELECT p_id, LEVEL_NUM, LEVEL_NAME FROM `sys_mac`.`mac_policy` WHERE`p_name` = POLICY_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_LEVEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_LEVEL`(
  `POLICY_NAME` VARCHAR(128),
  `LEVEL_NAME`  VARCHAR(128),
  `NEW_NAME`    VARCHAR(128))
BEGIN
  DECLARE level_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name already exits", MYSQL_ERRNO=7701;
    ELSE
      RESIGNAL;
    END IF;
  END;


  START TRANSACTION;
  IF NEW_NAME = '' OR NEW_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name should not be empty", MYSQL_ERRNO=7701;
  END IF;
  SELECT LOCATE(':',NEW_NAME) + LOCATE(',',NEW_NAME) INTO level_state;
  IF level_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level name should not contains ; or ,", MYSQL_ERRNO=7701;
  END IF;

  SELECT COUNT(*) INTO level_state FROM `sys_mac`.`mac_level` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON 
              t2.p_name = POLICY_NAME AND t1.p_id = t2.p_id AND t1.l_name = LEVEL_NAME FOR UPDATE;
  IF level_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level not exits", MYSQL_ERRNO=7701;
  END IF;
  UPDATE `sys_mac`.`mac_level` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON t1.p_id = t2.p_id AND 
                  t2.p_name = POLICY_NAME SET t1.l_name = NEW_NAME WHERE t1.l_name = LEVEL_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_LEVEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_LEVEL`(
  `POLICY_NAME` VARCHAR(128),
  `LEVEL_NAME`  VARCHAR(128))
BEGIN
  DECLARE level_state INT DEFAULT 0;
  DECLARE level_num INT DEFAULT 0;
  DECLARE policy_id INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SELECT COUNT(*) INTO level_state FROM `sys_mac`.`mac_level` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON 
              t2.p_name = POLICY_NAME AND t1.p_id = t2.p_id AND t1.l_name = LEVEL_NAME FOR UPDATE;
  IF level_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level not exits", MYSQL_ERRNO=7701;
  END IF;

  SELECT t1.l_num, t1.p_id INTO level_num, policy_id FROM `sys_mac`.`mac_level` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON 
              t2.p_name = POLICY_NAME AND t1.p_id = t2.p_id AND t1.l_name = LEVEL_NAME FOR UPDATE;
    
  SELECT COUNT(*) INTO level_state FROM `sys_mac`.`mac_labels` WHERE 
              `p_id` = policy_id AND sys_mac.MAC_FIND_LEVEL(level_num, `label`) = 1;
  
  IF level_state > 0  THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the level is being used, connot be deleted", MYSQL_ERRNO=7701;
  END IF;
  DELETE FROM `sys_mac`.`mac_level` WHERE `p_id` = policy_id AND `l_num` = level_num;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_COMPARTMENT` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_COMPARTMENT`(
  `POLICY_NAME`  VARCHAR(128),
  `COMPART_NAME` VARCHAR(128))
BEGIN
  DECLARE compart_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment already exits", MYSQL_ERRNO=7702;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF COMPART_NAME = '' OR  COMPART_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment name should not be empty", MYSQL_ERRNO=7702;
  END IF;
  SELECT LOCATE(':',COMPART_NAME) + LOCATE(',',COMPART_NAME) INTO compart_state;
  IF compart_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment name should not contains ; or ,", MYSQL_ERRNO=7702;
  END IF;

  SELECT COUNT(*) INTO compart_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF compart_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7702;
  END IF;
  INSERT INTO `sys_mac`.`mac_compartment` SELECT p_id, NULL, COMPART_NAME FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_COMPARTMENT` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_COMPARTMENT`(
  `POLICY_NAME`  VARCHAR(128),
  `COMPART_NAME` VARCHAR(128),
  `NEW_NAME`     VARCHAR(128))
BEGIN
  DECLARE compart_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment already exits", MYSQL_ERRNO=7702;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF NEW_NAME = '' OR NEW_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment name should not be empty", MYSQL_ERRNO=7702;
  END IF;
  SELECT LOCATE(':',NEW_NAME) + LOCATE(',',NEW_NAME) INTO compart_state;
  IF compart_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment name should not contains ; or ,", MYSQL_ERRNO=7702;
  END IF;

  SELECT COUNT(*) INTO compart_state FROM `sys_mac`.`mac_compartment` AS t1 JOIN `sys_mac`.`mac_policy` AS t2
                    ON t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.c_name = COMPART_NAME FOR UPDATE;
  IF compart_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment not exits", MYSQL_ERRNO=7702;
  END IF;
  UPDATE `sys_mac`.`mac_compartment` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON t1.p_id = t2.p_id AND 
                  t2.p_name = POLICY_NAME SET t1.c_name = NEW_NAME WHERE t1.c_name = COMPART_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_COMPARTMENT` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_COMPARTMENT`(
  `POLICY_NAME`   VARCHAR(128),
  `COMPART_NAME`  VARCHAR(128))
BEGIN
  DECLARE compart_state INT DEFAULT 0;
  DECLARE compart_id INT DEFAULT 0;
  DECLARE policy_id INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;
  
  START TRANSACTION;
  SELECT COUNT(*) INTO compart_state FROM `sys_mac`.`mac_compartment` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
          t1.p_id = t2.p_id AND t1.c_name = COMPART_NAME AND t2.p_name = POLICY_NAME FOR UPDATE;
  IF compart_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment not exits", MYSQL_ERRNO=7702;
  END IF;
  SELECT t1.c_id, t1.p_id INTO compart_id, policy_id FROM `sys_mac`.`mac_compartment` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
          t1.p_id = t2.p_id AND t1.c_name = COMPART_NAME AND t2.p_name = POLICY_NAME;
  
  SELECT COUNT(*) INTO compart_state FROM `sys_mac`.`mac_labels` WHERE 
                  `p_id` = policy_id AND sys_mac.MAC_FIND_COMPART(compart_id, `label`) = 1;
  IF compart_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the compartment is being used, connot be deleted", MYSQL_ERRNO=7702;
  END IF;
  DELETE FROM `sys_mac`.`mac_compartment` WHERE `p_id` = policy_id AND `c_id` = compart_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_GROUP` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_GROUP`(
  `POLICY_NAME` VARCHAR(128),
  `GROUP_NAME`  VARCHAR(128),
  `PARENT_NAME` VARCHAR(128))
BEGIN
  DECLARE group_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group already exits", MYSQL_ERRNO=7703;
    ELSE
      RESIGNAL;
    END IF;
  END;
  
  START TRANSACTION;
  IF GROUP_NAME = '' OR  GROUP_NAME IS NULL OR PARENT_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group name should not be empty", MYSQL_ERRNO=7703;
  END IF;
  SELECT LOCATE(':',GROUP_NAME) + LOCATE(',',GROUP_NAME) INTO group_state;
  IF group_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group name should not contains ; or ,", MYSQL_ERRNO=7703;
  END IF;

  SELECT COUNT(*) INTO group_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF group_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7703;
  END IF;
  IF PARENT_NAME IS NOT NULL THEN
    SELECT COUNT(*) INTO group_state FROM  `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                              t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = PARENT_NAME FOR UPDATE;
    IF group_state = 0 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the parent group not exits", MYSQL_ERRNO=7703;
    END IF;
    INSERT INTO `sys_mac`.`mac_group` SELECT t1.p_id, NULL, GROUP_NAME, g_id FROM `sys_mac`.`mac_group` AS t1 
                                              JOIN `sys_mac`.`mac_policy` AS t2 ON t1.p_id = t2.p_id AND 
                                              t2.p_name = POLICY_NAME AND t1.g_name = PARENT_NAME;
  ELSE
    INSERT INTO `sys_mac`.`mac_group` SELECT p_id, NULL, GROUP_NAME, NULL FROM 
                                              `sys_mac`.`mac_policy` AS t1 WHERE t1.p_name = POLICY_NAME;
  END IF;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_GROUP` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_GROUP`(
  `POLICY_NAME`  VARCHAR(128),
  `GROUP_NAME`   VARCHAR(128),
  `NEW_NAME`     VARCHAR(128))
BEGIN
  DECLARE group_state INT DEFAULT 0;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group already exits", MYSQL_ERRNO=7703;
    ELSE
      RESIGNAL;
    END IF;
  END;
  

  START TRANSACTION;
  IF NEW_NAME = '' OR NEW_NAME IS NULL THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group name should not be empty", MYSQL_ERRNO=7703;
  END IF;
  SELECT LOCATE(':',NEW_NAME) + LOCATE(',',NEW_NAME) INTO group_state;
  IF group_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group name should not contains ; or ,", MYSQL_ERRNO=7703;
  END IF;

  SELECT COUNT(*) INTO group_state FROM `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                              t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = GROUP_NAME FOR UPDATE;
  IF group_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group not exits", MYSQL_ERRNO=7703;
  END IF;
  UPDATE `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME 
            SET t1.g_name = NEW_NAME WHERE t1.g_name = GROUP_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_GROUP_PARENT` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_GROUP_PARENT`(
  `POLICY_NAME` VARCHAR(128),
  `GROUP_NAME`  VARCHAR(128),
  `PARENT_NAME` VARCHAR(128))
BEGIN
  DECLARE group_state INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF PARENT_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the parent group name shuold be provided", MYSQL_ERRNO=7703;
  END IF;

  SELECT COUNT(*) INTO group_state FROM  `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                              t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = GROUP_NAME FOR UPDATE;
  IF group_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group not exits", MYSQL_ERRNO=7703;
  ELSE
    SET @policy_id = 0;
    SELECT `p_id` INTO @policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME;
    IF PARENT_NAME IS NOT NULL THEN
      SELECT COUNT(*) INTO group_state FROM  `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                              t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = PARENT_NAME FOR UPDATE;
      IF group_state = 0 THEN
        SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the parent group not exits", MYSQL_ERRNO=7703;
      ELSE
        SET @parent_id = 0;
        SET @group_name = PARENT_NAME;
        IF PARENT_NAME = GROUP_NAME THEN
          SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group have closed loop", MYSQL_ERRNO=7703;
        END IF;
        SELECT `parent_id` INTO @parent_id FROM `sys_mac`.`mac_group` WHERE `p_id` = @policy_id AND `g_name` = PARENT_NAME;
        WHILE @parent_id IS NOT NULL DO
          SELECT `parent_id`, `g_name`  INTO @parent_id, @group_name FROM `sys_mac`.`mac_group` 
                                                              WHERE `p_id` = @policy_id AND `g_id`= @parent_id;
          IF @group_name = GROUP_NAME THEN
            SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group have closed loop", MYSQL_ERRNO=7703;
          END IF;
        END WHILE;
        UPDATE `sys_mac`.`mac_group` AS t1, (SELECT g_id FROM `sys_mac`.`mac_group` WHERE `p_id` = @policy_id AND `g_name` = PARENT_NAME) AS t2
                                                  SET t1.parent_id = t2.g_id WHERE t1.p_id = @policy_id AND t1.g_name = GROUP_NAME; 
      END IF;
    ELSE
      UPDATE `sys_mac`.`mac_group` SET `parent_id` = NULL WHERE `p_id` = @policy_id AND `g_name` = GROUP_NAME;
    END IF;
  END IF;
  COMMIT;  
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_GROUP` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_GROUP`(
  `POLICY_NAME`   VARCHAR(128),
  `GROUP_NAME`  VARCHAR(128))
BEGIN
  DECLARE group_state INT DEFAULT 0;
  DECLARE policy_id INT DEFAULT 0;
  DECLARE group_id INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SELECT COUNT(*) INTO group_state FROM `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                              t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = GROUP_NAME FOR UPDATE;
  IF group_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group not exits", MYSQL_ERRNO=7703;
  END IF;
  SELECT t1.p_id, t1.g_id INTO policy_id, group_id FROM `sys_mac`.`mac_group` AS t1 JOIN `sys_mac`.`mac_policy` AS t2 ON
                                            t1.p_id = t2.p_id AND t2.p_name = POLICY_NAME AND t1.g_name = GROUP_NAME;

  SELECT COUNT(*) INTO group_state FROM `sys_mac`.`mac_group` WHERE `p_id` = policy_id AND `parent_id` =  group_id;
  IF group_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group has child, connot be deleted", MYSQL_ERRNO=7703;
  END IF;
  SELECT COUNT(*) INTO group_state FROM `sys_mac`.`mac_labels` WHERE 
                    `p_id` = policy_id AND sys_mac.MAC_FIND_GROUP(group_id, `label`) = 1;
  IF group_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the group is being used, connot be deleted", MYSQL_ERRNO=7703;
  END IF;
  DELETE FROM `sys_mac`.`mac_group` WHERE `g_id` = group_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_TRANSFOR_LABEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_TRANSFOR_LABEL`(
  `POLICY_ID`     INT,
  `LABEL_VALUE`   VARCHAR(600),
  OUT OUT_LABEL   VARCHAR(600))
BEGIN
  DECLARE level_val VARCHAR(600);
  DECLARE compart_arry VARCHAR(600);
  DECLARE group_arry VARCHAR(600);
  DECLARE label_arry VARCHAR(600) DEFAULT NULL;
  DECLARE label_state INT DEFAULT 0;
  DECLARE com_num INT DEFAULT 0;
  DECLARE group_id INT DEFAULT 0;
  DECLARE compart_id INT DEFAULT 0;
  DECLARE it INT DEFAULT 0;
  DECLARE err_msg TEXT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SET OUT_LABEL = NULL;
  /*check the params*/
  IF LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label should not be empty", MYSQL_ERRNO=7705;
  END IF;

  SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_policy` WHERE `p_id` = POLICY_ID FOR UPDATE;
  IF label_state = 0 THEN
    SET err_msg = CONCAT('the policy id ', POLICY_ID, ' not exits');
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
  END IF;
  /*check the label formart 'L;C;G',splict the label value to level arry, compartment arry, group arry*/
  SET com_num = LENGTH(LABEL_VALUE) - LENGTH(REPLACE(LABEL_VALUE,':',''));
  IF com_num != 2 THEN
    set err_msg = CONCAT('the label ', LABEL_VALUE, ' not meet the format requirements');
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
  END IF;
  SELECT SUBSTRING_INDEX(LABEL_VALUE,':', 1) INTO level_val;
  SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(LABEL_VALUE,':', 2),':',-1) INTO compart_arry;
  SELECT SUBSTRING_INDEX(LABEL_VALUE,':',-1) INTO group_arry;

  IF level_val IS NULL OR level_val = '' THEN
    set err_msg = CONCAT('the label ', LABEL_VALUE, ' not meet the format requirements');
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
  END IF;
  /*deal with the level */
  SET @level_num = 0;
  SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_level` 
                          WHERE `p_id` = POLICY_ID AND `l_name` = level_val FOR UPDATE;
  IF label_state = 0 THEN
    set err_msg = CONCAT('the level ', level_val, ' not exits');
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
  ELSE
    SELECT `l_num` INTO @level_num FROM `sys_mac`.`mac_level` 
                          WHERE `p_id` = POLICY_ID AND `l_name` = level_val FOR UPDATE;
    SET label_arry = @level_num;
  END IF;
  DROP TABLE IF EXISTS `sys_mac`.`mac_tmp_list`;
  CREATE TEMPORARY TABLE `sys_mac`.`mac_tmp_list`(`id` int);
  SELECT CONCAT(label_arry, ':') INTO label_arry;
  /*deal with the compartment*/
  IF compart_arry IS NOT NULL AND compart_arry != '' THEN
    SET it = 1;
    SET compart_id = 0;
    SET com_num = LENGTH(compart_arry) - LENGTH(REPLACE(compart_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET @compart_name = SUBSTRING_INDEX(SUBSTRING_INDEX(compart_arry, ',', it), ',', -1);
      IF @compart_name IS NULL OR @compart_name = '' THEN
        DROP TABLE `sys_mac`.`mac_tmp_list`;
        set err_msg = CONCAT('the label ', LABEL_VALUE, ' not meet the format requirements');
        SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
      END IF;
      SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_compartment` 
                            WHERE `p_id` = POLICY_ID AND `c_name` = @compart_name FOR UPDATE;
      IF label_state = 0 THEN
        DROP TABLE `sys_mac`.`mac_tmp_list`;
        set err_msg = CONCAT('the commpartment ', @compart_name, ' not exits');
        SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
      ELSE
        SELECT `c_id` INTO compart_id FROM `sys_mac`.`mac_compartment` 
                          WHERE `p_id` = POLICY_ID AND `c_name` = @compart_name FOR UPDATE;
        SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_tmp_list` WHERE id = compart_id;
        IF label_state > 0 THEN
          DROP TABLE `sys_mac`.`mac_tmp_list`;
          set err_msg = CONCAT('there exits same compartment in ', LABEL_VALUE);
          SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
        ELSE
          INSERT INTO `sys_mac`.`mac_tmp_list` values(compart_id);
        END IF;
      END IF;
      SET it = it + 1;
    END WHILE;
    SELECT CONCAT(label_arry, GROUP_CONCAT(`id` ORDER BY `id` ASC separator ',')) INTO label_arry FROM `sys_mac`.`mac_tmp_list`;
  END IF;
  SELECT CONCAT(label_arry, ':') INTO label_arry;
  DELETE FROM `sys_mac`.`mac_tmp_list`;
  /*deal with the group*/
  IF group_arry IS NOT NULL AND group_arry != '' THEN
    SET it = 1;
    SET group_id = 0;
    SET com_num = LENGTH(group_arry) - LENGTH(REPLACE(group_arry,',','')) + 1;
    WHILE it <= com_num DO
      SET @group_name = SUBSTRING_INDEX(SUBSTRING_INDEX(group_arry, ',', it), ',', -1);
      IF @group_name IS NULL OR @group_name = '' THEN
        DROP TABLE `sys_mac`.`mac_tmp_list`;
        set err_msg = CONCAT('the label ', LABEL_VALUE, ' not meet the format requirements');
        SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
      END IF;
      SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_group` 
                            WHERE `p_id` = POLICY_ID AND `g_name` = @group_name FOR UPDATE;
      IF label_state = 0 THEN
        DROP TABLE `sys_mac`.`mac_tmp_list`;
        set err_msg = CONCAT('the group ', @group_name, ' not exits');
        SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
      ELSE
        SELECT `g_id` INTO group_id FROM `sys_mac`.`mac_group` 
                          WHERE `p_id` = POLICY_ID AND `g_name` = @group_name FOR UPDATE;
        SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_tmp_list` WHERE id = group_id;
        IF label_state > 0 THEN
          DROP TABLE `sys_mac`.`mac_tmp_list`;
          SET err_msg = CONCAT('there exits same group in ', LABEL_VALUE);
          SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT=err_msg, MYSQL_ERRNO=7705;
        ELSE
          INSERT INTO `sys_mac`.`mac_tmp_list` values(group_id);
        END IF;
      END IF;
      SET it = it + 1;
    END WHILE;
    SELECT CONCAT(label_arry, GROUP_CONCAT(`id` ORDER BY `id` ASC separator ',')) INTO label_arry FROM `sys_mac`.`mac_tmp_list`;
  END IF;
  SET OUT_LABEL = label_arry;
  DROP TABLE `sys_mac`.`mac_tmp_list`;
  COMMIT;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_LABEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_LABEL`(
  `POLICY_NAME` VARCHAR(128),
  `LABEL_VALUE` VARCHAR(600))
BEGIN
  DECLARE label_arry VARCHAR(600) DEFAULT NULL;
  DECLARE policy_id INT;
  DECLARE label_state INT;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label already exits", MYSQL_ERRNO=7704;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  /*check the params*/
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name or label value should be provided", MYSQL_ERRNO=7704;
  END IF;

  /*check the policy or label exit*/
  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO label_state, policy_id FROM `sys_mac`.`mac_policy`
                                   WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF label_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7704;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, label_arry);

  INSERT INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, label_arry);
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_CREATE_LABEL_USE_ID` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_CREATE_LABEL_USE_ID`(
  `LABEL_ID` INT,
  `POLICY_NAME` VARCHAR(128),
  `LABEL_VALUE` VARCHAR(600))
BEGIN
  DECLARE label_arry VARCHAR(600) DEFAULT NULL;
  DECLARE policy_id INT;
  DECLARE label_state INT;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label already exits", MYSQL_ERRNO=7704;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF LABEL_ID < 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label id must bigger than zero", MYSQL_ERRNO=7704;
  END IF;
  /*check the params*/
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name or label value should be provided", MYSQL_ERRNO=7704;
  END IF;

  /*check the policy or label exit*/
  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO label_state, policy_id FROM `sys_mac`.`mac_policy`
                                   WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF label_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7704;
  END IF;
  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, label_arry);
  INSERT INTO `sys_mac`.`mac_labels` VALUES(LABEL_ID, policy_id, label_arry);
  COMMIT;
  FLUSH PRIVILEGES;
END $$


DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_LABEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_LABEL`(
  `POLICY_NAME` VARCHAR(128),
  `LABEL_VALUE` VARCHAR(600),
  `NEW_VALUE`   VARCHAR(600))
BEGIN
  DECLARE label_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE new_label VARCHAR(600) DEFAULT NULL;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label already exits", MYSQL_ERRNO=7704;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name or orgin label value should be provided", MYSQL_ERRNO=7704;
  END IF;
  IF NEW_VALUE IS NULL OR NEW_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the new label value must be provied", MYSQL_ERRNO=7704;
  END IF;
  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO label_state, policy_id FROM `sys_mac`.`mac_policy`
                                   WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF label_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7704;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO label_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF label_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the old label not exit", MYSQL_ERRNO=7704;
  END IF;
  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, NEW_VALUE, new_label);
  UPDATE `sys_mac`.`mac_labels` SET `label` = new_label WHERE `p_id` = policy_id AND `label` = org_label;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_LABEL` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_LABEL`(
  `POLICY_NAME`   VARCHAR(128),
  `LABEL_VALUE`   VARCHAR(128))
BEGIN
  DECLARE label_state INT;
  DECLARE policy_id INT;
  DECLARE label_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name or label value should be provided", MYSQL_ERRNO=7704;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO label_state, policy_id FROM `sys_mac`.`mac_policy`
                                   WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF label_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7704;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);
  
  SELECT COUNT(*), ANY_VALUE(`l_id`) INTO label_state, label_id FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF label_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exit", MYSQL_ERRNO=7704;
  END IF;
  
  SELECT SUM(a) into label_state FROM (SELECT COUNT(*) a FROM `sys_mac`.`mac_column_policy` WHERE `l_id` = label_id UNION ALL
                                       SELECT COUNT(*) a FROM `sys_mac`.`mac_table_policy` WHERE `l_id` = label_id UNION ALL
                                       SELECT COUNT(*) a FROM `sys_mac`.`mac_database_policy` WHERE `l_id` = label_id UNION ALL
                                       SELECT COUNT(*) a FROM `sys_mac`.`mac_row_policy` WHERE `l_id` = label_id UNION ALL
                                       SELECT COUNT(*) a FROM `sys_mac`.`mac_user_policy` WHERE `p_id` = policy_id  AND (`read_label` = org_label OR 
                                       `write_label` = org_label OR `def_read_label` = org_label OR `def_write_label` = org_label OR `def_row_label` = org_label)) AS tab;
  IF label_state > 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label is being used, connot be deleted", MYSQL_ERRNO=7704;
  END IF;
  DELETE FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label;
  COMMIT;
  FLUSH PRIVILEGES;
END $$
 
DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_APPLY_DATABASE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_APPLY_DATABASE_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128),
  `LABEL_VALUE`     VARCHAR(600))
BEGIN
  DECLARE db_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database have same policy already", MYSQL_ERRNO=7706;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR POLICY_NAME IS NULL OR POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and policy name should not be empty", MYSQL_ERRNO=7706;
  END IF;
  IF LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label should not be empty", MYSQL_ERRNO=7706;
  END IF;

  IF V_DB_NAME = 'sys_mac' OR V_DB_NAME = 'information_schema' OR V_DB_NAME = 'mysql'
                            OR V_DB_NAME = 'sys' OR  V_DB_NAME = 'peformance_schema' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database connot be system database", MYSQL_ERRNO=7706;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO db_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7706;
  END IF;
  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME AND `p_id` = policy_id FOR UPDATE;
  IF db_state > 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database just allow only one label with same policy", MYSQL_ERRNO=7706;
  END IF;
  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7706;
  END IF;
  INSERT INTO `sys_mac`.`mac_database_policy` SELECT V_DB_NAME, policy_id, `l_id` FROM 
                `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_DATABASE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_DATABASE_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `POLICY_NAME`   VARCHAR(128),
  `LABEL_VALUE`    VARCHAR(600))
BEGIN
  DECLARE db_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR POLICY_NAME IS NULL OR POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and policy name should not be empty", MYSQL_ERRNO=7706;
  END IF;
  IF LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label should not be empty", MYSQL_ERRNO=7706;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
  END IF;

  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits policy", MYSQL_ERRNO=7706;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO db_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7706;
  END IF;
  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME AND `p_id` = policy_id FOR UPDATE;
  IF db_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not apply this policy", MYSQL_ERRNO=7706;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7706;
  END IF;
  UPDATE `sys_mac`.`mac_database_policy` AS t1, (SELECT l_id FROM `sys_mac`.`mac_labels` 
                WHERE `p_id` = policy_id AND `label` = org_label) AS t2 SET t1.l_id = t2.l_id 
                WHERE `p_id` = policy_id AND `db_name` = V_DB_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_DATABASE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_DATABASE_POLICY`(
  `V_DB_NAME`     VARCHAR(64),
  `POLICY_NAME`   VARCHAR(128))
BEGIN
  DECLARE db_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;
  START TRANSACTION;

  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR POLICY_NAME IS NULL OR POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and policy name should not be empty", MYSQL_ERRNO=7706;
  END IF;
  
  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
  END IF;
  
  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits policy", MYSQL_ERRNO=7706;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO db_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF db_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7706;
  END IF;
  SELECT COUNT(*) INTO db_state FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME AND `p_id` = policy_id FOR UPDATE;
  IF db_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not apply this policy", MYSQL_ERRNO=7706;
  END IF;
  DELETE FROM `sys_mac`.`mac_database_policy` WHERE `db_name` = V_DB_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_APPLY_TABLE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_APPLY_TABLE_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128),
  `LABEL_VALUE`     VARCHAR(600))
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table have same policy already", MYSQL_ERRNO=7707;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7707;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7707;
  END IF;

  IF V_DB_NAME = 'sys_mac' OR V_DB_NAME = 'information_schema' OR V_DB_NAME = 'mysql'
                           OR V_DB_NAME = 'sys' OR  V_DB_NAME = 'peformance_schema' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table connot be system table", MYSQL_ERRNO=7707;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7707;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7707;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7707;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_table_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state > 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table just allow only one label with same policy", MYSQL_ERRNO=7707;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7707;
  END IF;
  INSERT INTO `sys_mac`.`mac_table_policy` SELECT V_DB_NAME, V_TABLE_NAME, policy_id, `l_id` FROM 
                `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_TABLE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_TABLE_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128),
  `LABEL_VALUE`     VARCHAR(600))
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7707;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name and label value should not be empty", MYSQL_ERRNO=7707;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_table_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits policy", MYSQL_ERRNO=7707;
  END IF;  

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7707;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_table_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` = V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not apply this policy", MYSQL_ERRNO=7707;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7707;
  END IF;
  UPDATE `sys_mac`.`mac_table_policy` AS t1, (SELECT l_id FROM `sys_mac`.`mac_labels` 
                WHERE `p_id` = policy_id AND `label` = org_label) AS t2 SET t1.l_id = t2.l_id
                WHERE `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_TABLE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_TABLE_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128))
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7707;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7707;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_table_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits policy", MYSQL_ERRNO=7707;
  END IF;  
    
  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7707;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_table_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not apply this policy", MYSQL_ERRNO=7707;
  END IF;
  DELETE FROM `sys_mac`.`mac_table_policy` WHERE `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_APPLY_COLUMN_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_APPLY_COLUMN_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_COLUMN_NAME`   VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128),
  `LABEL_VALUE`     VARCHAR(600))
BEGIN
  DECLARE column_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column have applied same policy already", MYSQL_ERRNO=7708;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' OR V_COLUMN_NAME IS NULL OR V_COLUMN_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database and table and column name should not be empty", MYSQL_ERRNO=7708;
  END IF;  
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7708;
  END IF;

  IF V_DB_NAME = 'sys_mac' OR V_DB_NAME = 'information_schema' OR V_DB_NAME = 'mysql'
                            OR V_DB_NAME = 'sys' OR  V_DB_NAME = 'peformance_schema' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table connot be system table", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `information_schema`.`COLUMNS` WHERE 
          `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column not exits", MYSQL_ERRNO=7708;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT lower(V_COLUMN_NAME) into V_COLUMN_NAME;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO column_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_column_policy` WHERE `db_name` = V_DB_NAME AND 
              `table_name` =  V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME AND `p_id` = policy_id;
  IF column_state > 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column just allow only one label with same policy", MYSQL_ERRNO=7708;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7708;
  END IF;
  INSERT INTO `sys_mac`.`mac_column_policy` SELECT V_DB_NAME, V_TABLE_NAME, V_COLUMN_NAME, policy_id, `l_id` FROM 
                `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_COLUMN_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_COLUMN_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_COLUMN_NAME`   VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128),
  `LABEL_VALUE`     VARCHAR(600))
BEGIN
  DECLARE column_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR 
     V_TABLE_NAME = '' OR V_COLUMN_NAME IS NULL OR V_COLUMN_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database and table and column name should not be empty", MYSQL_ERRNO=7708;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR LABEL_VALUE IS NULL OR LABEL_VALUE = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7708;
  END IF;

  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_column_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME FOR UPDATE;
  IF column_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column not exits policy", MYSQL_ERRNO=7708;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT lower(V_COLUMN_NAME) into V_COLUMN_NAME;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO column_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_column_policy` WHERE `db_name` = V_DB_NAME AND 
              `table_name` =  V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME AND `p_id` = policy_id FOR UPDATE;
  IF column_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column not apply this policy", MYSQL_ERRNO=7708;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, LABEL_VALUE, org_label);

  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7708;
  END IF;
  UPDATE `sys_mac`.`mac_column_policy` AS t1, (SELECT l_id FROM `sys_mac`.`mac_labels` 
                WHERE `p_id` = policy_id AND `label` = org_label) AS t2 SET t1.l_id = t2.l_id
                WHERE `db_name` = V_DB_NAME AND `table_name` = V_TABLE_NAME AND
                       `COLUMN_NAME` = V_COLUMN_NAME AND`p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_COLUMN_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_COLUMN_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_COLUMN_NAME`   VARCHAR(64),
  `POLICY_NAME`     VARCHAR(128))
BEGIN
  DECLARE column_state INT;
  DECLARE policy_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' OR V_COLUMN_NAME IS NULL OR V_COLUMN_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database and table and column name should not be empty", MYSQL_ERRNO=7708;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = ''THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7708;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT lower(V_COLUMN_NAME) into V_COLUMN_NAME;

  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_column_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME FOR UPDATE;
  IF column_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column not exits policy", MYSQL_ERRNO=7708;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO column_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF column_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7708;
  END IF;
  SELECT COUNT(*) INTO column_state FROM `sys_mac`.`mac_column_policy` WHERE `db_name` = V_DB_NAME AND 
              `table_name` =  V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME AND `p_id` = policy_id FOR UPDATE;
  IF column_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the column not apply this policy", MYSQL_ERRNO=7708;
  END IF;
  DELETE FROM `sys_mac`.`mac_column_policy`  WHERE `db_name` = V_DB_NAME AND 
    `table_name` = V_TABLE_NAME AND `COLUMN_NAME` = V_COLUMN_NAME AND`p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_APPLY_USER_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_APPLY_USER_POLICY`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255),
  `POLICY_NAME`     VARCHAR(128),
  `READ_LABEL`      VARCHAR(600),
  `WRITE_LABEL`     VARCHAR(600),
  `DEF_LABEL`       VARCHAR(600),
  `ROW_LABEL`       VARCHAR(600))
PRO:BEGIN
  DECLARE user_state INT;
  DECLARE policy_id INT;
  DECLARE read_org_label  VARCHAR(600) DEFAULT NULL;
  DECLARE write_org_label VARCHAR(600) DEFAULT NULL;
  DECLARE def_org_label   VARCHAR(600) DEFAULT NULL;
  DECLARE row_org_label   VARCHAR(600) DEFAULT NULL;
  DECLARE def_write_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;
  
  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR READ_LABEL IS NULL OR READ_LABEL = '' OR
     WRITE_LABEL IS NULL OR WRITE_LABEL = '' OR DEF_LABEL IS NULL OR DEF_LABEL = '' OR 
     ROW_LABEL IS NULL OR ROW_LABEL = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO user_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7709;
  END IF;
  SELECT COUNT(*) INTO user_state FROM `sys_mac`.`mac_user_policy` WHERE `user` = V_USER AND 
              `host` = V_HOST AND `p_id` = policy_id;
  IF user_state > 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user just allow only one label with same policy", MYSQL_ERRNO=7709;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, READ_LABEL, read_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, WRITE_LABEL, write_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, DEF_LABEL, def_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, ROW_LABEL, row_org_label);
  
  SELECT CONVERT(SUBSTRING_INDEX(read_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the write_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(read_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(row_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(row_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;


  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(read_org_label, write_org_label)) = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the write_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(read_org_label, def_org_label)) = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(def_org_label, row_org_label)) = 0  OR 
     (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(write_org_label, row_org_label)) = 0 THEN
     SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONCAT(SUBSTRING_INDEX(write_org_label, ':', 1), sys_mac.MAC_GET_LABEL_CONTINAS(def_org_label, write_org_label)) INTO def_write_label;

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, read_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, write_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, def_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, row_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, def_write_label);

  REPLACE INTO `sys_mac`.`mac_user_policy` VALUES (V_USER, V_HOST, policy_id, read_org_label,
                           write_org_label, def_org_label,  def_write_label, row_org_label);
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ALTER_USER_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ALTER_USER_POLICY`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255),
  `POLICY_NAME`     VARCHAR(128),
  `READ_LABEL`      VARCHAR(600),
  `WRITE_LABEL`     VARCHAR(600),
  `DEF_LABEL`       VARCHAR(600),
  `ROW_LABEL`       VARCHAR(600))
BEGIN
  DECLARE user_state INT;
  DECLARE policy_id INT;
  DECLARE read_org_label  VARCHAR(600) DEFAULT NULL;
  DECLARE write_org_label VARCHAR(600) DEFAULT NULL;
  DECLARE def_org_label   VARCHAR(600) DEFAULT NULL;
  DECLARE row_org_label   VARCHAR(600) DEFAULT NULL;
  DECLARE def_write_label VARCHAR(600) DEFAULT NULL;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' OR READ_LABEL IS NULL OR READ_LABEL = '' OR
     WRITE_LABEL IS NULL OR WRITE_LABEL = '' OR DEF_LABEL IS NULL OR DEF_LABEL = '' OR 
     ROW_LABEL IS NULL OR ROW_LABEL = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO user_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7709;
  END IF;

  SELECT COUNT(*) INTO user_state FROM `sys_mac`.`mac_user_policy` WHERE `user` = V_USER AND 
              `host` = V_HOST AND `p_id` = policy_id FOR UPDATE;
  IF user_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not apply this policy", MYSQL_ERRNO=7709;
  END IF;

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, READ_LABEL, read_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, WRITE_LABEL, write_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, DEF_LABEL, def_org_label);

  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, ROW_LABEL, row_org_label);
  
  SELECT CONVERT(SUBSTRING_INDEX(read_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the write_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(read_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(def_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(row_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONVERT(SUBSTRING_INDEX(row_org_label,':', 1), SIGNED) >= CONVERT(SUBSTRING_INDEX(write_org_label,':', 1), SIGNED) INTO user_state;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;


  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(read_org_label, write_org_label)) = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the write_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(read_org_label, def_org_label)) = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the def_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  IF (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(def_org_label, row_org_label)) = 0  OR 
     (SELECT sys_mac.MAC_CHECK_LABEL_CONTINAS(write_org_label, row_org_label)) = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the row_label is invalid", MYSQL_ERRNO=7709;
  END IF;

  SELECT CONCAT(SUBSTRING_INDEX(write_org_label, ':', 1), sys_mac.MAC_GET_LABEL_CONTINAS(def_org_label, write_org_label)) INTO def_write_label;

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, read_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, write_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, def_org_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, def_write_label);

  INSERT IGNORE INTO `sys_mac`.`mac_labels` VALUES(NULL, policy_id, row_org_label);

  REPLACE INTO `sys_mac`.`mac_user_policy` VALUES (V_USER, V_HOST, policy_id, read_org_label,
                           write_org_label, def_org_label,  def_write_label, row_org_label);
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_USER_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_USER_POLICY`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255),
  `POLICY_NAME`     VARCHAR(128))
BEGIN
  DECLARE user_state INT;
  DECLARE user_privs INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO user_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7709;
  END IF;
  SELECT COUNT(*) INTO user_state FROM `sys_mac`.`mac_user_policy` WHERE `user` = V_USER AND `host` = V_HOST AND `p_id` = policy_id FOR UPDATE;
  SELECT COUNT(*) INTO user_privs FROM `sys_mac`.`mac_user_privs` WHERE `user` = V_USER AND `host` = V_HOST AND `p_id` = policy_id FOR UPDATE;
  IF user_state = 0 AND user_privs = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not apply this policy", MYSQL_ERRNO=7709;
  END IF;
  DELETE FROM `sys_mac`.`mac_user_policy` WHERE `user` = V_USER AND `host` = V_HOST AND `p_id` = policy_id;
  DELETE FROM `sys_mac`.`mac_user_privs` WHERE `user` = V_USER AND `host` = V_HOST AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_APPLY_ROW_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_APPLY_ROW_POLICY`(
  `V_DB_NAME`         VARCHAR(64), 
  `V_TABLE_NAME`      VARCHAR(64),
  `V_POLICY_NAME`     VARCHAR(128),
  `V_LABEL`           VARCHAR(600),
  `V_OPTION_VISIBLE`  INT)
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE label_id INT;
  DECLARE org_label VARCHAR(600) DEFAULT NULL;
  DECLARE errno INT DEFAULT 0;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    GET STACKED DIAGNOSTICS CONDITION 1
      errno = MYSQL_ERRNO;
    IF errno = 1062 THEN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row have applied same policy already", MYSQL_ERRNO=7710;
    ELSE
      RESIGNAL;
    END IF;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7710;
  END IF;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' OR V_LABEL IS NULL OR V_LABEL = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy and label value should not be empty", MYSQL_ERRNO=7710;
  END IF;
  IF V_OPTION_VISIBLE < 0 OR V_OPTION_VISIBLE > 1 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the disable option only allow 0 or 1", MYSQL_ERRNO=7710;
  END IF;

  IF V_DB_NAME = 'sys_mac' OR V_DB_NAME = 'information_schema' OR V_DB_NAME = 'mysql'
                           OR V_DB_NAME = 'sys' OR  V_DB_NAME = 'peformance_schema' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="not support system database", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7710;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7710;
  END IF;
  
  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7710;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state > 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row just allow only one label with same policy", MYSQL_ERRNO=7710;
  END IF;
  CALL `sys_mac`.`MAC_TRANSFOR_LABEL`(policy_id, V_LABEL, org_label);

  SELECT COUNT(*) , ANY_VALUE(`l_id`) INTO table_state, label_id FROM `sys_mac`.`mac_labels` WHERE `p_id` = policy_id AND `label` = org_label FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the label not exits", MYSQL_ERRNO=7710;
  END IF;
  SET @column = CONCAT("_gdb_mac_policy_", policy_id);
  
  INSERT INTO `sys_mac`.`mac_row_policy`(`db_name`, `table_name`, `column_name`, `p_id`, `l_id`, `visible_option`) 
                          SELECT V_DB_NAME, V_TABLE_NAME, @column, policy_id, label_id, V_OPTION_VISIBLE;
  COMMIT;
  FLUSH PRIVILEGES;

  SET @cmd = CONCAT("ALTER TABLE ", V_DB_NAME, ".", V_TABLE_NAME, " ADD COLUMN ", 
                    "_gdb_mac_policy_", policy_id, " INT NOT NULL DEFAULT ", label_id);
  PREPARE stmt FROM @cmd;
  EXECUTE stmt;
  DROP PREPARE stmt;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DROP_ROW_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DROP_ROW_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_POLICY_NAME`   VARCHAR(128))
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7710;
  END IF;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy should not be empty", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7710;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7710;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not exits policy", MYSQL_ERRNO=7710;
  END IF;  

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not apply this policy", MYSQL_ERRNO=7710;
  END IF;
  DELETE FROM `sys_mac`.`mac_row_policy` WHERE `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;

  SET @cmd = CONCAT("ALTER TABLE ", V_DB_NAME, ".", V_TABLE_NAME, " DROP COLUMN ", "_gdb_mac_policy_", policy_id);
  PREPARE stmt FROM @cmd;
  EXECUTE stmt;
  DROP PREPARE stmt;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ENABLE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ENABLE_POLICY`(
  `V_POLICY_NAME`   VARCHAR(128)
  )
BEGIN
  DECLARE policy_state INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7700;
  END IF;
  
  SELECT COUNT(*) INTO policy_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF policy_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7700;
  END IF;

  UPDATE `sys_mac`.`mac_policy` SET `enable` = TRUE WHERE `p_name` = V_POLICY_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DISABLE_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DISABLE_POLICY`(
  `V_POLICY_NAME`   VARCHAR(128)
  )
BEGIN
  DECLARE policy_state INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy name should not be empty", MYSQL_ERRNO=7700;
  END IF;
  
  SELECT COUNT(*) INTO policy_state FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF policy_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7700;
  END IF;

  UPDATE `sys_mac`.`mac_policy` SET `enable` = FALSE WHERE `p_name` = V_POLICY_NAME;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_ENABLE_ROW_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_ENABLE_ROW_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_POLICY_NAME`   VARCHAR(128)
  )
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7710;
  END IF;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy should not be empty", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7710;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7710;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not exits policy", MYSQL_ERRNO=7710;
  END IF;  

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not apply this policy", MYSQL_ERRNO=7710;
  END IF;
  
  UPDATE `sys_mac`.`mac_row_policy` SET `enable` = TRUE WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_DISABLE_ROW_POLICY` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_DISABLE_ROW_POLICY`(
  `V_DB_NAME`       VARCHAR(64),
  `V_TABLE_NAME`    VARCHAR(64),
  `V_POLICY_NAME`   VARCHAR(128)
  )
BEGIN
  DECLARE table_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_DB_NAME IS NULL OR V_DB_NAME = '' OR V_TABLE_NAME IS NULL OR V_TABLE_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database name and table name should not be empty", MYSQL_ERRNO=7710;
  END IF;
  IF V_POLICY_NAME IS NULL OR V_POLICY_NAME = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy should not be empty", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = V_DB_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the database not exits", MYSQL_ERRNO=7710;
  END IF;
  SELECT COUNT(*) INTO table_state FROM `information_schema`.`TABLES` WHERE 
                    `TABLE_SCHEMA` = V_DB_NAME AND `TABLE_NAME` = V_TABLE_NAME;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table not exits", MYSQL_ERRNO=7710;
  END IF;

  IF (SELECT @@lower_case_table_names > 0 ) THEN
    SELECT lower(V_DB_NAME) into V_DB_NAME;
    SELECT lower(V_TABLE_NAME) into V_TABLE_NAME;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO table_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = V_POLICY_NAME FOR UPDATE;
  IF table_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                                        `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not exits policy", MYSQL_ERRNO=7710;
  END IF;

  SELECT COUNT(*) INTO table_state FROM `sys_mac`.`mac_row_policy` WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id FOR UPDATE;
  IF table_state = 0 THEN 
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the table row not apply this policy", MYSQL_ERRNO=7710;
  END IF;
  
  UPDATE `sys_mac`.`mac_row_policy` SET `enable` = FALSE WHERE 
                    `db_name` = V_DB_NAME AND `table_name` =  V_TABLE_NAME AND `p_id` = policy_id;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_SET_USER_PRIV` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_SET_USER_PRIV`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255),
  `POLICY_NAME`     VARCHAR(128),
  `V_PRIVS`         enum('read', 'full'))
BEGIN
  DECLARE user_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  IF POLICY_NAME IS NULL OR POLICY_NAME = ''THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  SELECT COUNT(*), ANY_VALUE(`p_id`) INTO user_state, policy_id FROM `sys_mac`.`mac_policy` WHERE `p_name` = POLICY_NAME FOR UPDATE;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the policy not exits", MYSQL_ERRNO=7709;
  END IF;
  REPLACE INTO `sys_mac`.`mac_user_privs` VALUES(V_USER, V_HOST, policy_id, V_PRIVS);
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_GRANT_USER_ALL_PRIVS` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_GRANT_USER_ALL_PRIVS`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255))
BEGIN
  DECLARE user_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;
  
  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  REPLACE INTO `sys_mac`.`mac_privileged_users` VALUES(V_USER, V_HOST);
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DROP PROCEDURE IF EXISTS `sys_mac`.`MAC_REVOKE_USER_ALL_PRIVS` $$
CREATE DEFINER='root'@'localhost' PROCEDURE `sys_mac`.`MAC_REVOKE_USER_ALL_PRIVS`(
  `V_USER`          VARCHAR(32),
  `V_HOST`          VARCHAR(255))
BEGIN
  DECLARE user_state INT;
  DECLARE policy_id INT;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  IF V_USER IS NULL OR V_USER = '' OR V_HOST IS NULL OR V_HOST = '' THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user and host should not be empty", MYSQL_ERRNO=7709;
  END IF;
  
  SELECT COUNT(*) INTO user_state FROM `mysql`.`user` WHERE `Host` = V_HOST AND `User` = V_USER;
  IF user_state = 0 THEN
    SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT="the user not exits", MYSQL_ERRNO=7709;
  END IF;

  DELETE FROM `sys_mac`.`mac_privileged_users` WHERE `user` = V_USER and `host` = V_HOST;
  COMMIT;
  FLUSH PRIVILEGES;
END $$

DELIMITER ;

-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
