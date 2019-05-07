CREATE TABLE IF NOT EXISTS `accounts` (
  `account` char(12) NOT NULL DEFAULT '',
  `balance` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`account`),
  KEY `idx_balance` (`balance`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `records` (
  `seq` bigint(20) NOT NULL DEFAULT '0',
  `from` char(12) NOT NULL DEFAULT '',
  `to` char(12) NOT NULL DEFAULT '',
  `quantity` bigint(20) NOT NULL DEFAULT '0',
  `memo` varchar(256) NOT NULL DEFAULT '',
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0: 已执行 1： 不可逆',
  `time` int(11) NOT NULL DEFAULT '0',
  `transaction_id` char(64) NOT NULL DEFAULT '',
  `block_id` char(64) NOT NULL DEFAULT '',
  `block_num` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`seq`),
  KEY `idx_from` (`from`),
  KEY `idx_to` (`to`),
  KEY `idx_time` (`time`),
  KEY `idx_transaction_id` (`transaction_id`),
  KEY `idx_block_id` (`block_id`)
) ENGINE=InnoDB AUTO_INCREMENT=29881297 DEFAULT CHARSET=utf8;
