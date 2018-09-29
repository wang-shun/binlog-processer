package com.datatrees.datacenter.core.task;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMqDispensor extends TaskDispensor {

  private ConnectionFactory factory;

  public RabbitMqDispensor() {
    factory = new ConnectionFactory();
    factory.setHost(__properties.getProperty("queue.server"));
    factory.setPort(Integer.parseInt(__properties.getProperty("queue.port")));
  }

  @Override
  public <T> void dispense(String topic, T binlog) {
    executorService.submit(() -> {
      Connection connection = null;
      Channel channel = null;
      try {
        logger.info(String.format("begin to dispense message [%s] to %s.", binlog, topic));
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(topic, false, false, false, null);
        channel.
          basicPublish("", topic, null,
            (binlog.getClass().isAssignableFrom(String.class)) ? ((String) binlog).
              getBytes("UTF-8")
              : JSON.toJSONString(binlog).getBytes("UTF-8"));
        logger.info(String.format("success to dispense message [%s] to %s.", binlog, topic));
      } catch (IOException e) {
        logger.error("error to publish rabbit mq message because of io error", e);
      } catch (TimeoutException e) {
        logger.error("error to publish rabbit mq message because of"
          + e.getMessage(), e);
      } finally {
        if (null != connection && connection.isOpen()) {
          try {
            connection.close();
          } catch (IOException e) {
            logger.error("error to close rabbit mq connection because of"
              + e.getMessage(), e);
          }
        }
        if (null != channel && channel.isOpen()) {
          try {
            channel.close();
          } catch (Exception e) {
            logger.error("error to close rabbit channel connection because of" + e.getMessage(),
              e);
          }
        }
      }
    });
  }

  @Override
  public void dispense(Binlog binlog) {
    String topic = __properties.getProperty("queue.topic");
    dispense(topic, binlog);
  }
}
