/*
供 otter 使用， otter 需要对 retl.* 的读写权限，以及对业务表的读写权限
1. 创建database retl
*/
CREATE DATABASE retl;

/* 2. 用户授权 给同步用户授权 */
CREATE USER retl@'%' IDENTIFIED BY 'retl';
GRANT USAGE ON *.* TO `retl`@'%';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `retl`@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `retl`.* TO `retl`@'%';
/* 业务表授权，这里可以限定只授权同步业务的表 */
GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO `retl`@'%';  

/* 3. 创建系统表 */
USE retl;
DROP TABLE IF EXISTS retl.retl_buffer;
DROP TABLE IF EXISTS retl.retl_mark;
DROP TABLE IF EXISTS retl.xdual;

CREATE TABLE retl_buffer
(	
	ID BIGINT(20) AUTO_INCREMENT,
	TABLE_ID INT(11) NOT NULL,
	FULL_NAME varchar(512),
	TYPE CHAR(1) NOT NULL,
	PK_DATA VARCHAR(256) NOT NULL,
	GMT_CREATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	GMT_MODIFIED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT RETL_BUFFER_ID PRIMARY KEY (ID) 
)  ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE retl_mark
(	
	ID BIGINT AUTO_INCREMENT,
	CHANNEL_ID INT(11),
	CHANNEL_INFO varchar(128),
	CONSTRAINT RETL_MARK_ID PRIMARY KEY (ID) 
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE xdual (
  ID BIGINT(20) NOT NULL AUTO_INCREMENT,
  X timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

/* 4. 插入初始化数据 */
INSERT INTO retl.xdual(id, x) VALUES (1,now()) ON DUPLICATE KEY UPDATE x = now();
