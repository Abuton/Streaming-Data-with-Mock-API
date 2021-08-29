CREATE TABLE IF NOT EXISTS `transaction` 
(
    `id` INT NOT NULL AUTO_INCREMENT,
    `txid` VARCHAR(50) NOT NULL,
    `uid` VARCHAR(50) NOT NULL,
    `amount` FLOAT NOT NULL,
    PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
USE 
