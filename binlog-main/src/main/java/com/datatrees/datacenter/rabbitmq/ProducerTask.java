package com.datatrees.datacenter.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author personalc
 */
public class ProducerTask extends RabbitMqTask {
    private static Logger LOG = LoggerFactory.getLogger(ProducerTask.class);

    public ProducerTask(String queue) {
        super(queue);
    }

    public void sendMessage(String fileInfo) {
        try {
            channel.basicPublish("", queue, null, fileInfo.getBytes());
            LOG.info("message " + fileInfo + "send to mq");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}