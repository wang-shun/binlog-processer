package com.datatrees.datacenter.main;

import com.datatrees.datacenter.rabbitmq.ConsumerTask;

/**
 * 自动检查和修复程序
 */
public class HiveCheckAndRepairAuto {
    public static void main(String[] args) {
         for(int i=0;i<4;i++) {
            ConsumerTask consumer = new ConsumerTask("hive_check_test");
             // TODO: 2018/10/20 改成线程池模式
            Thread thread = new Thread(consumer);
            thread.start();
        }
    }
}
