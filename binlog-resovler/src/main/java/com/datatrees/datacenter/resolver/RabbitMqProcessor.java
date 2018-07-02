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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public class RabbitMqProcessor extends TaskProcessor {

  private ConnectionFactory factory;
  private Semaphore semaphore;
  private Queue<String> taskQueue;
  private ThreadPoolExecutor threadPoolExecutor;
  private ScheduledExecutorService scheduledExecutorService;

  public RabbitMqProcessor() {
    factory = new ConnectionFactory();
    factory.setHost(properties.getProperty("queue.server"));
    factory.setPort(Integer.parseInt(properties.getProperty("queue.port")));
    semaphore = new Semaphore(MAX_THREAD_BINLOG_THREAD, true);
    taskQueue = new LinkedBlockingDeque<>();

    logger.info("Runtime processors =" + Runtime.getRuntime().availableProcessors());
    int availableThread = Integer.valueOf(
      MAX_THREAD_BINLOG_THREAD > Runtime.getRuntime().availableProcessors() ? Runtime.getRuntime()
        .availableProcessors() : MAX_THREAD_BINLOG_THREAD);

    threadPoolExecutor = new ThreadPoolExecutor(availableThread, availableThread, 0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>());
    scheduledExecutorService = Executors.newScheduledThreadPool(10);
    scheduledExecutorService.scheduleAtFixedRate(this::report, 10, 10, TimeUnit.MINUTES);
    super.taskProcessorListner = this::onMessageReceived;
  }

  private void onMessageReceived(String taskDesc) {
    Binlog task = JSON.parseObject(taskDesc, Binlog.class);
    try {
      logger.info("start to read task desc step1:" + taskDesc);
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
  }

  private void onListen(String message) {
    if (super.taskProcessorListner != null) {
      try {
        super.taskProcessorListner.onMessageReceived(message);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  private void report() {
    DBbiz.report(this.topic, taskQueue.size(), threadPoolExecutor.getTaskCount(),
      threadPoolExecutor.getCompletedTaskCount(), threadPoolExecutor.getPoolSize(),
      threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getActiveCount(),
      threadPoolExecutor.getMaximumPoolSize(), semaphore.availablePermits(),
      semaphore.getQueueLength());
  }

  private void acquireSemaphore() {
    try {
      logger.info("try acquire semaphore");
      if (semaphore.tryAcquire(30, TimeUnit.MINUTES)) {
        logger.info("try acquire semaphore successed within 30 minutes.");
      } else {
        logger.info("try acquire semaphore failed within 30 minutes.");
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private void releaseSemaphore() {
    try {
      semaphore.release();
    } catch (Exception e) {
      logger.error("error to release.");
    }
  }

  private void processTaskQueue() {
    do {
      if (!taskQueue.isEmpty()) {
        acquireSemaphore();
        String taskDesc = taskQueue.poll();
        logger.info("success to get task desc:" + taskDesc + " from local queue.");
        logger.info(String.format(
          "[task count:%d-completed task count:%d-pool size:%d-core pool size:%d-active count:%d-maximum pool size:%d]",
          threadPoolExecutor.getTaskCount(), threadPoolExecutor.getCompletedTaskCount(),
          threadPoolExecutor.getPoolSize(), threadPoolExecutor.getCorePoolSize(),
          threadPoolExecutor.getActiveCount(), threadPoolExecutor.getMaximumPoolSize()
        ));
        threadPoolExecutor.submit(() -> {
          onListen(taskDesc);
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
    logger.info("success to get task from rabbitmq " + task);
    taskQueue.offer(task);
    logger.info("success to offer task to local queue " + task);
  }

  @Override
  public void run() {
    String topic =
      StringUtils.isEmpty(this.topic) ? properties.getProperty("queue.topic") : this.topic;
    Connection connection = null;
    Channel channel = null;
    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(topic, false, false, false, null);
      channel
        .basicConsume(topic, true, new DefaultConsumer(channel) {
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
    } finally {
      Thread localThread = new Thread(this::processTaskQueue);
      localThread.start();
    }
  }
}