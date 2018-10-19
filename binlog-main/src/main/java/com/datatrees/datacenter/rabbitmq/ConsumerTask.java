package com.datatrees.datacenter.rabbitmq;

import com.datatrees.datacenter.compare.HiveCompareByFile;
import com.datatrees.datacenter.repair.hive.HiveDataRepair;
import com.datatrees.datacenter.table.CheckResult;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * @author personalc
 */
public class ConsumerTask extends RabbitMqTask implements Runnable, Consumer {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerTask.class);
    HiveCompareByFile hiveCompareByFile = new HiveCompareByFile();
    HiveDataRepair hiveDataRepair = new HiveDataRepair();

    public ConsumerTask(String queue) {
        super(queue);
    }

    @Override
    public void run() {
        try {
            //start consuming messages. Auto acknowledge messages.
            channel.basicConsume(queue, false, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Called when consumer is registered.
     */
    @Override
    public void handleConsumeOk(String consumerTag) {
        LOG.info("Consumer " + consumerTag + " registered");
    }

    /**
     * Called when new message is available.
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope env,
                               BasicProperties props, byte[] body) {
        try {
            String fileInfo = new String(body, "UTF-8");
            String[] info = fileInfo.split(":");
            String dbInstance=info[0];
            String dataBase=info[1];
            String tableName=info[2];
            String partition=info[3];
            String partitionType=info[4];
            String fileName=info[5];
            hiveCompareByFile.compareOnePartition(dbInstance, dataBase, tableName, partition, partitionType, fileName);
            channel.basicAck(env.getDeliveryTag(), false);
            LOG.info("Message:" + fileInfo + " received.");
            CheckResult checkResult=new CheckResult();
            checkResult.setDbInstance(dbInstance);
            checkResult.setTableName(tableName);
            checkResult.setDataBase(dataBase);
            checkResult.setPartitionType(partitionType);
            checkResult.setFilePartition(partition);
            checkResult.setFileName(fileName);
            //hiveDataRepair.repairByIdList(checkResult,"t_binlog_check_hive_copy");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleCancel(String consumerTag) {
    }

    @Override
    public void handleCancelOk(String consumerTag) {
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException arg1) {
    }

}
