DROP TABLE IF EXISTS `WialonRetranslator`;
CREATE TABLE `WialonRetranslator` (
	`imei` BIGINT(15) UNSIGNED NOT NULL,
	`ip` VARCHAR(50) NOT NULL,
	`port` SMALLINT(5) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `EgtsRetranslator`;
CREATE TABLE `EgtsRetranslator` (
	`imei` BIGINT(15) UNSIGNED NOT NULL,
	`ip` VARCHAR(50) NOT NULL,
	`port` SMALLINT(5) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `sent_id`;
CREATE TABLE `sent_id` (
	`id` BIGINT(15) UNSIGNED,
	`ip` VARCHAR(50),
	`port` SMALLINT(5) UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `wialonretranslator` (`imei`, `ip`, `port`)
SELECT `imei`, `ret_ip`, `ret_port` FROM `devices` WHERE `ret_type`="WialonRetranslator" AND `ret_ip`!='' AND `ret_port`>0;

INSERT INTO `egtsretranslator` (`imei`, `ip`, `port`)
SELECT `imei`, `ret_ip`, `ret_port` FROM `devices` WHERE `ret_type`="EgtsRetranslator" AND `ret_ip`!='' AND `ret_port`>0;

INSERT INTO `wialonretranslator` (`imei`, `ip`, `port`)
SELECT `imei`, `ret2_ip`, `ret2_port` FROM `devices` WHERE `ret2_type`="WialonRetranslator" AND `ret2_ip`!='' AND `ret2_port`>0;

INSERT INTO `egtsretranslator` (`imei`, `ip`, `port`)
SELECT `imei`, `ret2_ip`, `ret2_port` FROM `devices` WHERE `ret2_type`="EgtsRetranslator" AND `ret2_ip`!='' AND `ret2_port`>0;

SET @MID = (SELECT MAX(`id`) FROM `geo_100`);
INSERT INTO `sent_id`(`ip`, `port`) SELECT DISTINCT `ip`, `port` FROM `egtsretranslator`;
INSERT INTO `sent_id`(`ip`, `port`) SELECT DISTINCT `ip`, `port` FROM `wialonretranslator`;
UPDATE `sent_id` SET `id`=@MID 