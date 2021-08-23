CREATE DATABASE IF NOT EXISTS `replicator` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
CREATE TABLE IF NOT EXISTS `replication` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `source_addr` varchar(255) NOT NULL DEFAULT '',
  `dest_addr` varchar(255) NOT NULL DEFAULT '',
  `pos` text,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `transaction_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `state` varchar(255) NOT NULL DEFAULT '',
  `message` text,
  `source_connect_params` varchar(1000) NOT NULL DEFAULT '',
  `dest_connect_params` varchar(1000) NOT NULL DEFAULT '',
  `filter` text,
  `kafka_params` varchar(1000) NOT NULL DEFAULT '',
  `service_addr` varchar(255) NOT NULL DEFAULT '',
  `rate_limit` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `copy_state` (
  `repl_id` int(11) NOT NULL DEFAULT '0',
  `dbname` varchar(128) NOT NULL DEFAULT '',
  `table_name` varchar(128) NOT NULL DEFAULT '',
  `lastpk` varbinary(2000) DEFAULT NULL,
  PRIMARY KEY (`repl_id`,``dbname,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
