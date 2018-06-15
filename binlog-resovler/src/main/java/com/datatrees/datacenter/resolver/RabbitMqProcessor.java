package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public class RabbitMqProcessor extends TaskProcessor {

  private ConnectionFactory factory;

  public RabbitMqProcessor() {
    factory = new ConnectionFactory();
    factory.setHost(properties.getProperty("queue.server"));
    factory.setPort(Integer.parseInt(properties.getProperty("queue.port")));
  }

  @Override
  public void run() {
    Connection connection = null;
    Channel channel = null;

    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(properties.getProperty("queue.topic"), false, false, false, null);
      channel
        .basicConsume(properties.getProperty("queue.topic"), false, new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body) throws IOException {
            if (body.length > 0) {
              String message = new String(body, "UTF-8");
              if (StringUtils.isNotBlank(message)) {
                executorService.submit(() -> {
                  try {
                    logger.info("start to read task desc " + message);
                    startRead(JSON.parseObject(message, Binlog.class));
                  } catch (Exception e) {
                    logger.error("error to consume message from rabbitmq because of "
                      + e.getMessage(), e
                    );
                  } finally {
                    try {
                      TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }
                });
              }
            }
          }
        });
    } catch (Exception e) {
      logger.error("error to consume message from rabbitmq because of "
        + e.getMessage(), e
      );
    }
  }
}