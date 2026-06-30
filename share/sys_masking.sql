-- set session greatdb_strict_create_table=off;
set @have_innodb= (select count(engine) from information_schema.engines where engine='INNODB' and support != 'NO');
set @is_encrypted = (select ENCRYPTION from information_schema.INNODB_TABLESPACES where NAME='mysql');
-- Tables below are NOT treated as DD tables by MySQL server yet.

SET FOREIGN_KEY_CHECKS= 1;
SET default_storage_engine=InnoDB;

SET NAMES utf8mb4;
SET @old_sql_mode = @@session.sql_mode, @@session.sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

DROP DATABASE IF EXISTS `sys_masking`;
CREATE DATABASE `sys_masking` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;


DELIMITER $$
DROP PROCEDURE IF EXISTS sys_masking.create_tablespace$$
CREATE DEFINER='root'@'localhost' PROCEDURE sys_masking.create_tablespace()
BEGIN
  DECLARE had_sys_tablespace INT DEFAULT 0;
  select count(*) into had_sys_tablespace from information_schema.innodb_tablespaces where NAME = 'gdb_sys_masking';
  IF had_sys_tablespace = 0 THEN
    CREATE TABLESPACE gdb_sys_masking ADD DATAFILE 'sys_masking.ibd' ENGINE = InnoDB;
  END IF;
  IF @is_encrypted = 'Y' THEN
    ALTER TABLESPACE gdb_sys_masking ENCRYPTION = 'Y';
  END IF;
END $$
CALL sys_masking.create_tablespace()$$
DROP PROCEDURE sys_masking.create_tablespace$$
DELIMITER ;

SET @cmd = " CREATE TABLE IF NOT EXISTS `sys_masking`.`masking_label` (
  `label_id` int NOT NULL AUTO_INCREMENT,
  `label_name` varchar(255) NOT NULL,
  `db_name` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `field_name` varchar(64) NOT NULL,
  UNIQUE KEY (`db_name`,`field_name`,`table_name`),
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`label_id`)
) ";
SET @str=IF(@have_innodb <> 0, CONCAT(@cmd, " ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=gdb_sys_masking ENCRYPTION='", @is_encrypted,"'"), CONCAT(@cmd, ' ENGINE= MYISAM'));
PREPARE stmt FROM @str;
EXECUTE stmt;

SET @cmd = " CREATE TABLE IF NOT EXISTS `sys_masking`.`masking_policy` (
  `polname` varchar(255) NOT NULL,
  `maskaction` enum('maskall','mask_inside') NOT NULL,
  `optinal` varchar(255) DEFAULT NULL,
  `polenabled` int NOT NULL DEFAULT 1,
  `create_time` timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`polname`)
) ";
SET @str=IF(@have_innodb <> 0, CONCAT(@cmd, " ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=gdb_sys_masking ENCRYPTION='", @is_encrypted,"'"), CONCAT(@cmd, ' ENGINE= MYISAM'));
PREPARE stmt FROM @str;
EXECUTE stmt;

SET @cmd = " CREATE TABLE IF NOT EXISTS `sys_masking`.`masking_policy_labels` (
  `polname` varchar(255) NOT NULL,
  `label_name` varchar(255) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`polname`,`label_name`),
  UNIQUE KEY `label_name` (`label_name`)
) ";
SET @str=IF(@have_innodb <> 0, CONCAT(@cmd, " ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=gdb_sys_masking ENCRYPTION='", @is_encrypted,"'"), CONCAT(@cmd, ' ENGINE= MYISAM'));
PREPARE stmt FROM @str;
EXECUTE stmt;


SET @cmd = " CREATE TABLE IF NOT EXISTS `sys_masking`.`masking_policy_users` (
  `polname` varchar(255) NOT NULL,
  `user_name` varchar(255) NOT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`polname`,`user_name`)
) ";
SET @str=IF(@have_innodb <> 0, CONCAT(@cmd, " ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=gdb_sys_masking ENCRYPTION='", @is_encrypted,"'"), CONCAT(@cmd, ' ENGINE= MYISAM'));
PREPARE stmt FROM @str;
EXECUTE stmt;

DROP PREPARE stmt;

DELIMITER ;;
DROP PROCEDURE IF EXISTS  `sys_masking`.`create_label` ;;

CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`create_label`(
    IN p_db_name VARCHAR(64),
    IN p_table_name VARCHAR(64),
    IN p_field_name VARCHAR(64),
    IN p_mask_label_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    DECLARE label_count INT;

    IF CHAR_LENGTH(p_db_name) < 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'db_name length must be greater than 1';
    END IF;

    IF CHAR_LENGTH(p_table_name) < 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'table_name length must be greater than 1';
    END IF;

    IF CHAR_LENGTH(p_field_name) < 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'field_name length must be greater than 1';
    END IF;

    IF CHAR_LENGTH(p_mask_label_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'mask_label_name length must be greater than 2';
    END IF;


    SELECT COUNT(*) INTO label_count
    FROM `sys_masking`.`masking_label`
    WHERE `db_name` = p_db_name and `table_name` = p_table_name and `field_name` = p_field_name;

    IF label_count > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'field has already define label';
    END IF;


    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

        INSERT INTO `sys_masking`.`masking_label` (`db_name`, `table_name`, `field_name`,`label_name`)
        VALUES (p_db_name, p_table_name, p_field_name, p_mask_label_name);

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`drop_label_by_id` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`drop_label_by_id`(
    IN p_label_id INT
)
BEGIN
    DECLARE old_autocommit INT;
    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

        DELETE FROM `sys_masking`.`masking_label` WHERE label_id = p_label_id;

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;
 
DROP PROCEDURE IF EXISTS  `sys_masking`.`drop_label_by_name` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`drop_label_by_name`(
    IN p_label_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    IF CHAR_LENGTH(p_label_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'label_name length must be greater than 2';
    END IF;


    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;
        DELETE FROM `sys_masking`.`masking_label` WHERE label_name = p_label_name;
        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;
END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`create_policy` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`create_policy`(
    IN p_policy_name VARCHAR(255),
    IN p_mask_action ENUM('maskall', 'mask_inside'),
    IN p_optional VARCHAR(255)
)
BEGIN
    DECLARE policy_count INT;
    DECLARE old_autocommit INT;

    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    SELECT COUNT(*) INTO policy_count
    FROM `sys_masking`.`masking_policy`
    WHERE `polname` = p_policy_name;

    IF policy_count > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name has already exists';
    END IF;

    IF p_mask_action = 'mask_inside' THEN
        IF p_optional IS NOT NULL AND NOT p_optional REGEXP '^[+-]?[0-9]+,[+-]?[0-9]+(,.*)?$' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid optional parameter format number,number,char';
        END IF;
    END IF;

    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    INSERT INTO `sys_masking`.`masking_policy` (`polname`, `maskaction`, `optinal`,`polenabled`)
    VALUES (p_policy_name, p_mask_action, p_optional, 1);

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`drop_policy` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`drop_policy`(
    IN p_policy_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    DELETE FROM `sys_masking`.`masking_policy_users` WHERE polname = p_policy_name;
    DELETE FROM `sys_masking`.`masking_policy_labels` WHERE polname = p_policy_name;
    DELETE FROM `sys_masking`.`masking_policy` WHERE polname = p_policy_name;
 
        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`policy_add_label` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`policy_add_label`(
    IN p_policy_name VARCHAR(255),
    IN p_label_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    DECLARE label_count int;
    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    IF CHAR_LENGTH(p_label_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'label_name length must be greater than 2';
    END IF;

    SELECT COUNT(*) INTO label_count
    FROM `sys_masking`.`masking_policy_labels`
    WHERE `label_name` =  p_label_name;

    IF label_count > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'label has already bind policy';
    END IF;

    SELECT COUNT(*) INTO label_count
    FROM `sys_masking`.`masking_policy`
    WHERE `polname` =  p_policy_name;

    IF label_count = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy has not create, please check';
    END IF;

    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    INSERT INTO `sys_masking`.`masking_policy_labels` (`polname`, `label_name`)
    VALUES (p_policy_name, p_label_name);

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`policy_delete_label` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`policy_delete_label`(
    IN p_policy_name VARCHAR(255),
    IN p_label_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;

    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    IF CHAR_LENGTH(p_label_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'label_name length must be greater than 2';
    END IF;
    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    DELETE FROM `sys_masking`.`masking_policy_labels`
    WHERE polname = p_policy_name AND label_name = p_label_name;

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;
 
DROP PROCEDURE IF EXISTS  `sys_masking`.`policy_add_user` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`policy_add_user`(
    IN p_policy_name VARCHAR(255),
    IN p_user_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    DECLARE polcount int;
    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    IF CHAR_LENGTH(p_user_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_user_name length must be greater than 2';
    END IF;

    SELECT COUNT(*) INTO polcount
    FROM `sys_masking`.`masking_policy`
    WHERE `polname` =  p_policy_name;

    IF polcount = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy has not create, please check';
    END IF;

    SELECT COUNT(*) INTO polcount from `masking_policy_users`
    where `polname` = p_policy_name and `user_name` = p_user_name;
    
    IF polcount > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'user has alread add, please check';
    END IF;

    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    INSERT INTO `sys_masking`.`masking_policy_users` (`polname`, `user_name`)
    VALUES (p_policy_name, p_user_name);


        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`policy_delete_user` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`policy_delete_user`(
    IN p_policy_name VARCHAR(255),
    IN p_user_name VARCHAR(255)
)
BEGIN
    DECLARE old_autocommit INT;
    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    IF CHAR_LENGTH(p_user_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_user_name length must be greater than 2';
    END IF;
    SELECT @@autocommit INTO old_autocommit;
    BEGIN 

        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

    DELETE FROM `sys_masking`.`masking_policy_users`
    WHERE polname = p_policy_name AND user_name = p_user_name;


        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;
END ;;

DROP PROCEDURE IF EXISTS  `sys_masking`.`policy_enable` ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_masking`.`policy_enable`(
    IN p_policy_name VARCHAR(255),
    IN p_policy_enabled INT
)
BEGIN
    DECLARE old_autocommit INT;
    IF CHAR_LENGTH(p_policy_name) <= 2 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'policy_name length must be greater than 2';
    END IF;

    SELECT @@autocommit INTO old_autocommit;
    BEGIN 
        DECLARE rows_affected INT;
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            ROLLBACK;
            SET autocommit = old_autocommit;
            RESIGNAL;
        END;
        set autocommit = 0;

        UPDATE `sys_masking`.`masking_policy`
        SET polenabled = p_policy_enabled
        WHERE polname = p_policy_name;

        SET rows_affected = ROW_COUNT();
        IF rows_affected > 0 THEN
            select 'update success' as message ;
        ELSE
            select 'update failed,please check' as message ;
        END IF;

        SET autocommit = old_autocommit;
        IF @@enable_data_masking THEN
            FLUSH PRIVILEGES;
        END IF;
    END;

END ;;

DELIMITER ;
-- should always at the end of this file
SET @@session.sql_mode = @old_sql_mode;
set persist enable_data_masking = on;
flush privileges;
