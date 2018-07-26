/*
SQLyog Ultimate v11.25 (64 bit)
MySQL - 5.6.26 : Database - log_analyze
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`log_analyze` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `log_analyze`;

/*Table structure for table `log_analyze_job` */

DROP TABLE IF EXISTS `log_analyze_job`;

CREATE TABLE `log_analyze_job` (
  `jobId` int(11) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `jobName` varchar(100) CHARACTER SET latin1 NOT NULL COMMENT '用户自定义的名称，命名规则为：业务简称_指标简称',
  `jobType` int(1) NOT NULL COMMENT '1:浏览日志、2:点击日志、3:搜索日志、4:购买日志',
  `businessId` int(11) NOT NULL COMMENT '所属业务线',
  `status` int(1) NOT NULL COMMENT '0:下线 、1:在线',
  `createUser` varchar(50) CHARACTER SET latin1 NOT NULL COMMENT '创建用户',
  `updateUser` varchar(50) CHARACTER SET latin1 NOT NULL COMMENT '修改用户',
  `createDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `updataDate` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '修改时间',
  PRIMARY KEY (`jobId`,`jobName`),
  KEY `jobId` (`jobId`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

/*Data for the table `log_analyze_job` */

insert  into `log_analyze_job`(`jobId`,`jobName`,`jobType`,`businessId`,`status`,`createUser`,`updateUser`,`createDate`,`updataDate`) values (1,'itcast_p1002',1,1,1,'maoxiangyi','maoxiangyi','2015-11-16 15:42:27','2015-11-16 15:42:23');

/*Table structure for table `log_analyze_job_condition` */

DROP TABLE IF EXISTS `log_analyze_job_condition`;

CREATE TABLE `log_analyze_job_condition` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `jobId` int(11) NOT NULL COMMENT '任务编号',
  `field` varchar(50) CHARACTER SET latin1 NOT NULL COMMENT '用来比较的字段名称',
  `value` varchar(250) CHARACTER SET latin1 NOT NULL COMMENT '参与比较的字段值',
  `compare` int(1) NOT NULL COMMENT '1:包含 2:等于',
  `createUser` varchar(50) CHARACTER SET latin1 NOT NULL COMMENT '创建用户',
  `updateUser` varchar(50) CHARACTER SET latin1 NOT NULL COMMENT '修改用户',
  `createDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `updateDate` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '修改时间',
  KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

/*Data for the table `log_analyze_job_condition` */

insert  into `log_analyze_job_condition`(`id`,`jobId`,`field`,`value`,`compare`,`createUser`,`updateUser`,`createDate`,`updateDate`) values (1,1,'requestUrl','http://www.itcast.cn/product?id=1002',1,'maoxiangyi','maoxiangyi','2015-11-16 16:19:25','2015-11-16 15:44:21'),(2,1,'referrerUrl','http://www.itcast.cn/',2,'maoxiangyi','maoxiangyi','2015-11-16 16:23:56','2015-11-16 15:46:14');

/*Table structure for table `log_analyze_job_nimute_append` */

DROP TABLE IF EXISTS `log_analyze_job_nimute_append`;

CREATE TABLE `log_analyze_job_nimute_append` (
  `indexName` varchar(100) CHARACTER SET latin1 DEFAULT NULL COMMENT '指标名称',
  `pv` int(11) DEFAULT NULL COMMENT 'pv的值',
  `uv` bigint(20) DEFAULT NULL COMMENT 'uv的值',
 `executeTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '执行时间',
  `createTime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '写入数据库的时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `log_analyze_job_nimute_append` */

insert  into `log_analyze_job_nimute_append`(`indexName`,`pv`,`uv`,`executeTime`,`createTime`) values ('indexName',11,12,'2015-11-17 15:02:57','2015-11-17 15:03:01'),('1212',11,11,'2015-11-17 15:39:32','2015-11-17 15:39:30'),('1',11,11,'2015-11-17 15:39:34','2015-11-17 15:39:32'),('1',1,111,'2015-11-17 15:39:41','2015-11-17 15:39:46');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
