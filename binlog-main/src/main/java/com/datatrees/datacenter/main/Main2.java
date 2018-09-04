package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Main2 {

    private static TaskRunner taskRunner;

    private static Logger logger = LoggerFactory.getLogger(Main2.class);

    public static void main(String[] args) {

//        while (true) {
//            String desc = RedisQueue.defaultQueue().poll();
//
//            if (StringUtils.isNotBlank(desc)) {
//                System.out.println(desc);
//                try {
//                    TimeUnit.MILLISECONDS.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
        /**
         * telmarketing
         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/telemarketing/mysql-bin.000075", "rm-bp1cowwkt73ni6271_mysql-bin.000075.tar", "telemarketing.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
//        /**
//         * rule-engine
//         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000171", "rm-bp1h5j9w2o9335zsn_mysql-bin.000171.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000885", "rm-bp1h5j9w2o9335zsn_mysql-bin.000885.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/rulesplatform/mysql-bin.000886", "rm-bp1h5j9w2o9335zsn_mysql-bin.000886.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
////
////        /**
////         * ecommerce
////         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000151", "rm-bp1p4s8pgekg2di55_mysql-bin.000151.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000571", "rm-bp1p4s8pgekg2di55_mysql-bin.000571.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/basisdataecommerce/mysql-bin.000572", "rm-bp1p4s8pgekg2di55_mysql-bin.000572.tar", "basisdataecommerce2.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//////
////        /**
////         * cost management
////         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/costmanagement/mysql-bin.000235", "rm-bp1dq0cr4kk7j85ff_mysql-bin.000235.tar", "costmanagement.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/costmanagement/mysql-bin.000236", "rm-bp1dq0cr4kk7j85ff_mysql-bin.000236.tar", "costmanagement.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
////        /**
////         * debtcollection
////         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000629", "rm-bp155hg16d7t501f6_mysql-bin.000629.tar", "debtcollection.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000633", "rm-bp155hg16d7t501f6_mysql-bin.000633.tar", "debtcollection.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000637", "rm-bp155hg16d7t501f6_mysql-bin.000637.tar", "debtcollection.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/debtcollection/mysql-bin.000639", "rm-bp155hg16d7t501f6_mysql-bin.000639.tar", "debtcollection.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
////        /**
////         * pointserver
////         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/pointserver/mysql-bin.000696", "rm-bp1k0lx0x43542h47_mysql-bin.000696.tar", "pointserver.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://dn0:8020/binlogfile/pointserver/mysql-bin.000698", "rm-bp1k0lx0x43542h47_mysql-bin.000698.tar", "pointserver.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();


//        new Thread(()->{
//
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/creditaudit/mysql-bin.000130_bak", "rm-bp1tcbap8lvphh08x_mysql-bin.000130_bak", "creditaudit.mysql.rds.aliyuncs.com");
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/creditaudit/mysql-bin.000130_bak", "rm-bp1tcbap8lvphh08x_mysql-bin.000130_bak", "creditaudit.mysql.rds.aliyuncs.com");
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/creditaudit/mysql-bin.000130.tar", "rm-bp1tcbap8lvphh08x_mysql-bin.000130.tar", "creditaudit.mysql.rds.aliyuncs.com");
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/customerservice/mysql-bin.000731", "rm-bp1d1ac1fa41jzf28_mysql-bin.000731", "customerservice.mysql.rds.aliyuncs.com");
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/pointserver/mysql-bin.000704", "rm-bp1k0lx0x43542h47_mysql-bin.000704", "pointserver.mysql.rds.aliyuncs.com");
////                        Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/pointserver/mysql-bin.000696", "rm-bp1k0lx0x43542h47_mysql-bin.000696", "pointserver.mysql.rds.aliyuncs.com");
//                        Binlog binlog = new Binlog("hdfs://localhost:9000/pc/rm-bp1cowwkt73ni6271/4615201/1528396112000-mysql-bin.000083.tar", "rm-bp1cowwkt73ni6271_1528396112000-mysql-bin.000083.tar", "telemarketing.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

        /**
         * operator
         */
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/binlogfile/operator/mysql-bin.000728.tar", "rm-bp1x3mt0ogici00l5_mysql-bin.000728.tar", "basisdataoperator3.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000107", "rm-bp1x3mt0ogici00l4_mysql-bin.000107.tar", "telemarketing.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000180", "rm-bp1x3mt0ogici00l4_mysql-bin.000180.tar", "debtcollection.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000139.tar", "rm-bp1x3mt0ogici00l4_mysql-bin.000137.tar", "telemarketing.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000151.tar", "rm-bp1x3mt0ogici00l4_mysql-bin.000137.tar", "basisdataoperator8.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();


//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000476.tar", "rm-bp1x3mt0ogici00l4_mysql-bin.000476.tar", "dataplatform.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000342.tar", "rm-bp1x3mt0ogici00l4_mysql-bin.000342.tar", "rulesplatform-three.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000327", "rm-bp1x3mt0ogici00l5_1529811602-mysql-bin.000270.tar", "basisdataoperator3.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();
//
//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/mysql-bin.000933", "rm-bp1py1768bl28z5zs_1529811602-mysql-bin.000933.tar", "basisdataoperator4.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

//        new Thread(() -> {
//            Binlog binlog = new Binlog("hdfs://localhost:9000/pc/1533220479-mysql-bin.001160", "rm-bp114u46sxgr719pf_1533220479-mysql-bin.001160", "acrm-usercenter.mysql.rds.aliyuncs.com");
//            TaskDispensor.defaultDispensor().dispense(binlog);
//        }).start();

      new Thread(() -> {
        Binlog binlog = new Binlog("hdfs://localhost:9000/pc/1535168038-mysql-bin.000951", "rdspp6t6sg70l3aw79gs_1535168038-mysql-bin.000951", "gongfudai.mysql.rds.aliyuncs.com");
        TaskDispensor.defaultDispensor().dispense(binlog);
      }).start();


    }
}
