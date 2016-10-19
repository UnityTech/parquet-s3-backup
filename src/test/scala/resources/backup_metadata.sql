CREATE SCHEMA IF NOT EXISTS backend;

DROP TABLE IF EXISTS `backup_metadata`;

CREATE TABLE `backup_metadata` (
  `id` varchar(255) NOT NULL DEFAULT '',
  `entries` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;