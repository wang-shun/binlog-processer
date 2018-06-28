package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public class RabbitMqProcessor extends TaskProcessor {

  private ConnectionFactory factory;
  private Semaphore semaphore;
  private Queue<String> taskQueue;

  public RabbitMqProcessor() {
    factory = new ConnectionFactory();
    factory.setHost(properties.getProperty("queue.server"));
    factory.setPort(Integer.parseInt(properties.getProperty("queue.port")));
    semaphore = new Semaphore(MAX_THREAD_BINLOG_THREAD, true);
    taskQueue = new LinkedBlockingDeque<>();
//    Thread localThread = new Thread(() -> processTaskQueue());
    Thread localThread = new Thread(this::processTaskQueue);
    localThread.start();
  }


  private void acquireSemaphore() {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }

  private void releaseSemaphore() {
    semaphore.release();
  }

  private void processTaskQueue() {
    do {
      if (!taskQueue.isEmpty()) {
        acquireSemaphore();

        String taskDesc = taskQueue.poll();
        executorService.submit(() -> {
          Binlog task = JSON.parseObject(taskDesc, Binlog.class);
          try {
            logger.info("start to read task desc " + taskDesc);
            startRead(task, this::releaseSemaphore);
          } catch (Exception e) {
            logger.error("error to start consume message from rabbitmq because of "
              + e.getMessage(), e
            );
            exceptionHandler.
              handle(task.getIdentity1(), new BinlogException(e.getMessage(),
                (e instanceof BinlogException) ? ((BinlogException) e).getStatus() : Status.OTHER,
                e));
          }
        });
      }
      try {
        TimeUnit.MILLISECONDS.sleep(500L);
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    } while (true);
  }

  private void enqueue(String task) {
    taskQueue.offer(task);
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
        .basicConsume(properties.getProperty("queue.topic"), true, new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
            if (body.length > 0) {
              String message = new String(body, "UTF-8");
              if (StringUtils.isNotBlank(message)) {
                enqueue(message);
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