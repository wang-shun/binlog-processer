package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.ReflectUtility;
import com.datatrees.datacenter.resolver.handler.ExceptionHandler;
import com.datatrees.datacenter.resolver.reader.BinlogFileReader;
import com.datatrees.datacenter.resolver.reader.DefaultEventListner;
import com.datatrees.datacenter.resolver.storage.HdfsStorage;
import com.datatrees.datacenter.resolver.storage.LinuxStorage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskProcessor implements TaskRunner, Runnable {

  protected static Logger logger = LoggerFactory.getLogger(TaskProcessor.class);
  protected static Properties properties;
  protected static Integer MAX_THREAD_BINLOG_THREAD;
  private static TaskProcessor __taskProcessor;

  static {
    properties = PropertiesUtility.defaultProperties();
    MAX_THREAD_BINLOG_THREAD = Integer.valueOf(properties.getProperty("max.thread.binlog.thread"));
  }

  protected ExceptionHandler exceptionHandler;
  /**
   * 最大单机跑n个binlogreader
   */
  protected ExecutorService executorService = Executors.
    newFixedThreadPool(Integer.valueOf(
      MAX_THREAD_BINLOG_THREAD > Runtime.getRuntime().availableProcessors() ? Runtime.getRuntime()
        .availableProcessors() : MAX_THREAD_BINLOG_THREAD),
      r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });
  protected String topic;
  protected TaskProcessorListner taskProcessorListner;

  private RBlockingQueue<String> blockingQueue;
  private FileStorage fileStorage;


  public TaskProcessor() {
    blockingQueue = RedisQueue.defaultQueue();
    fileStorage = new HdfsStorage(new LinuxStorage(), true);// TODO: 2018/6/1 reflect from config
    exceptionHandler = (file, e) -> {
      DBbiz.update(file, e.getMessage(), e.getStatus());
    };
  }

  public static TaskProcessor defaultProcessor() {
    synchronized (TaskProcessor.class) {
      if (__taskProcessor == null) {
        Properties properties = PropertiesUtility.defaultProperties();
        __taskProcessor = ReflectUtility.reflect(properties.getProperty("queue.process.class"));
        if (__taskProcessor == null) {
          __taskProcessor = new RabbitMqProcessor();
        }
      }
      return __taskProcessor;
    }
  }

  public TaskProcessor setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public TaskProcessor registerListner(TaskProcessorListner listner) {
    this.taskProcessorListner = listner;
    return this;
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (!blockingQueue.isEmpty()) {
          String taskDesc = consumeTask();
          if (StringUtils.isNotBlank(taskDesc)) {
            logger.info(String.format("start to read log file of %s", taskDesc));
            executorService.submit(() -> {
              Binlog task = JSON.parseObject(taskDesc, Binlog.class);
              try {
                startRead(task, () -> {
                });
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
                exceptionHandler.handle(task.getIdentity1(), new BinlogException(e.getMessage(),
                  Status.OTHER, e));
              }
            });
          }
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      } finally {
        try {
          TimeUnit.MILLISECONDS.sleep(1000L);
        } catch (InterruptedException e) {
          logger.error(String.format("internal sleep failed."));
        }
      }
    }
  }

  protected String consumeTask() {
    return blockingQueue.poll();
  }

  @Override
  public void process() {
    Thread runThread = new Thread(this);
    runThread.start();
  }

  protected void startRead(Binlog task, Runnable r) throws IOException {
    try {
      logger.
        info("start to read task desc step2:" + task.toString());
      InputStream file = fileStorage.openReader(task.getPath());
      logger.
        info("success to open task desc step3:" + task.toString() + " file:" + file);
      BinlogFileReader binlogFileReader = new BinlogFileReader(task, file,
        new DefaultEventListner.InnerEventListner(fileStorage, task), null, exceptionHandler, r);
      logger.
        info("end to open file:" + task.toString());

      binlogFileReader.read();
    } catch (Exception e) {
      r.run();
      throw e;
    }
  }

  static class KafkaProcessor extends TaskProcessor {

    @Override
    protected String consumeTask() {
      return super.consumeTask();
    }
  }
}
