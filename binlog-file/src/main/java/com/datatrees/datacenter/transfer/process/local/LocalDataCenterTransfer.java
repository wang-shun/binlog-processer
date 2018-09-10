package com.datatrees.datacenter.transfer.process.local;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.process.BinlogFileTransfer;
import com.datatrees.datacenter.transfer.process.threadmanager.ThreadPoolInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class LocalDataCenterTransfer implements TaskRunner, BinlogFileTransfer {
    private static Logger LOG = LoggerFactory.getLogger(LocalDataCenterTransfer.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String[] Ips = properties.getProperty("SERVER_IP", "10.1.3.115").split(",");


    @Override
    public void process() {

    }

    @Override
    public void transfer() {
        ExecutorService executorService=ThreadPoolInstance.getExecutors();
        for (String ip : Ips) {
            RemoteBinlogOperate remoteBinlogOperate = new RemoteBinlogOperate();
            remoteBinlogOperate.setHostIp(ip.trim());
            executorService.execute(remoteBinlogOperate);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }
}
