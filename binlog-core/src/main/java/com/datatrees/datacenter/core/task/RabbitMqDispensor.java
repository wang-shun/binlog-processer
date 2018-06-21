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
  public void dispense(Binlog binlog) {
    executorService.submit(() -> {
      Connection connection = null;
      Channel channel = null;
      try {
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(__properties.getProperty("queue.topic"), false, false, false, null);
        channel.basicPublish("", __properties.getProperty("queue.topic"),
          null, JSON.toJSONString(binlog).getBytes("UTF-8"));
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
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
}
