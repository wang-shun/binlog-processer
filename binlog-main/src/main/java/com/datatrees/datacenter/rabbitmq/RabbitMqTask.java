package com.datatrees.datacenter.rabbitmq;


import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;


/**
 * @author personalc
 */
public abstract class RabbitMqTask {
    private static Logger LOG = LoggerFactory.getLogger(RabbitMqTask.class);
    private static Properties properties = PropertiesUtility.defaultProperties();

    private String hostServer = properties.getProperty("queue.server");
    private String port = properties.getProperty("queue.port");
    Channel channel;
    private Connection connection;
    String queue;

    public RabbitMqTask(String queue) {
        this.queue = queue;

        //Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();

        //hostname of your rabbitmq server
        factory.setHost(hostServer);
        factory.setPort(Integer.parseInt(port));

        //getting a connection
        try {
            connection = factory.newConnection();
            //creating a channel
            channel = connection.createChannel();
            //declaring a queue for this channel. If queue does not exist,
            //it will be created on the server.
            channel.queueDeclare(queue, false, false, false, null);
            channel.basicQos(10);
        } catch (TimeoutException ex) {
            LOG.info("can't get connection");
            connection = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭channel和connection。并非必须，因为隐含是自动调用的。
     *
     * @throws IOException
     */
    public void close() {
        try {
            this.channel.close();
        } catch (TimeoutException ex) {
            Log.info("channel close failed because of :" + ex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            this.connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}