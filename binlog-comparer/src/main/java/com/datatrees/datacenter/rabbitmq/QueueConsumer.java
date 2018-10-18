package com.datatrees.datacenter.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatrees.datacenter.datareader.AvroDataReader;
import org.apache.commons.lang.SerializationUtils;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueueConsumer extends EndPoint implements Runnable, Consumer {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);

    public QueueConsumer(String endPointName) throws IOException {
        super(endPointName);
    }

    @Override
    public void run() {
        try {
            //start consuming messages. Auto acknowledge messages.
            channel.basicConsume(endPointName, false, this);
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
                               BasicProperties props, byte[] body) throws IOException {
        String fileName = (String) SerializationUtils.deserialize(body);
        channel.basicAck(env.getDeliveryTag(), false);
        LOG.info("Message Number " + fileName+ " received.");

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

    public static void main(String[] args) {
        try {
            QueueConsumer queueConsumer = new QueueConsumer("hive_check_test");
            Thread thread = new Thread(queueConsumer);
            thread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
