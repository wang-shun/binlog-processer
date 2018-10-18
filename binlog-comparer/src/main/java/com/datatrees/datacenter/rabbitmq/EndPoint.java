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
 * Represents a connection with a queue
 *
 * @author syntx
 */
public abstract class EndPoint {
    private static Logger LOG = LoggerFactory.getLogger(EndPoint.class);
    protected static Properties properties=PropertiesUtility.defaultProperties();

    protected String hostServer=properties.getProperty("queue.server");
    protected String port=properties.getProperty("queue.port");
    protected Channel channel;
    protected Connection connection;
    protected String endPointName;

    public EndPoint(String endpointName) throws IOException {
        this.endPointName = endpointName;

        //Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();

        //hostname of your rabbitmq server
        factory.setHost(hostServer);
        factory.setPort(Integer.parseInt(port));

        //getting a connection
        try {
            connection = factory.newConnection();
        } catch (TimeoutException ex) {
            LOG.info("can't get connection");
            connection = null;
        }

        //creating a channel
        channel = connection.createChannel();

        //declaring a queue for this channel. If queue does not exist,
        //it will be created on the server.
        channel.queueDeclare(endpointName, false, false, false, null);
    }


    /**
     * 关闭channel和connection。并非必须，因为隐含是自动调用的。
     *
     * @throws IOException
     */
    public void close() throws IOException {
        try {
            this.channel.close();
        } catch (TimeoutException ex) {
            Log.info("channel close failed because of :" + ex);
        }
        this.connection.close();
    }
}