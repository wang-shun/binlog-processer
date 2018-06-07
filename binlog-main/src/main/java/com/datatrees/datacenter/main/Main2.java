package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main2 {

    private static TaskRunner taskRunner;

    private static Logger logger = LoggerFactory.getLogger(Main2.class);

    public static void main(String[] args) {
        /**
         * telmarketing
         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/telemarketing/mysql-bin.000075", "rm-bp1cowwkt73ni6271_mysql-bin.000075.tar", "telemarketing.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

//        /**
//         * rule-engine
//         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000171", "rm-bp1h5j9w2o9335zsn_mysql-bin.000171.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000885", "rm-bp1h5j9w2o9335zsn_mysql-bin.000885.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000886", "rm-bp1h5j9w2o9335zsn_mysql-bin.000886.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
//
//        /**
//         * ecommerce
//         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000151", "rm-bp1p4s8pgekg2di55_mysql-bin.000151.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000571", "rm-bp1p4s8pgekg2di55_mysql-bin.000571.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000572", "rm-bp1p4s8pgekg2di55_mysql-bin.000572.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
////
//        /**
//         * cost management
//         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/costmanagement/mysql-bin.000235", "rm-bp1dq0cr4kk7j85ff_mysql-bin.000235.tar", "costmanagement.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/costmanagement/mysql-bin.000236", "rm-bp1dq0cr4kk7j85ff_mysql-bin.000236.tar", "costmanagement.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

//        /**
//         * debtcollection
//         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000629", "rm-bp155hg16d7t501f6_mysql-bin.000629.tar", "debtcollection.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
//
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000633", "rm-bp155hg16d7t501f6_mysql-bin.000633.tar", "debtcollection.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000637", "rm-bp155hg16d7t501f6_mysql-bin.000637.tar", "debtcollection.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
//
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000639", "rm-bp155hg16d7t501f6_mysql-bin.000639.tar", "debtcollection.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

//        /**
//         * pointserver
//         */
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/pointserver/mysql-bin.000696", "rm-bp1k0lx0x43542h47_mysql-bin.000696.tar", "pointserver.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();
        new Thread(() -> {
            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/pointserver/mysql-bin.000698", "rm-bp1k0lx0x43542h47_mysql-bin.000698.tar", "pointserver.mysql.rds.aliyuncs.com");
            TaskDispensor.defaultDispensor().dispense(binlog);
        }).start();

    }
}
