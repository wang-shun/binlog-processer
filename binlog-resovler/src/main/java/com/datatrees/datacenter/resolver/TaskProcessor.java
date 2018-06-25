package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.ReflectUtility;
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
  private static TaskProcessor __taskProcessor;

  static {
    properties = PropertiesUtility.defaultProperties();
  }

  /**
   * 最大单机跑n个binlogreader
   */
  protected ExecutorService executorService = Executors.
    newFixedThreadPool(Integer.valueOf(properties.getProperty("max.thread.binlog.thread")),
      r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });
  private RBlockingQueue<String> blockingQueue;
  private FileStorage fileStorage;

  public TaskProcessor() {
    blockingQueue = RedisQueue.defaultQueue();
    fileStorage = new HdfsStorage(new LinuxStorage(), true);// TODO: 2018/6/1 reflect from config
  }

  public static TaskProcessor defaultProcessor() {
    synchronized (TaskProcessor.class) {
      if (__taskProcessor == null) {
        Properties properties = PropertiesUtility.defaultProperties();
        __taskProcessor = ReflectUtility.reflect(properties.getProperty("queue.dispense.class"));
        if (__taskProcessor == null) {
          __taskProcessor = new RabbitMqProcessor();
        }
      }
      return __taskProcessor;
    }
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
              try {
                startRead(JSON.parseObject(taskDesc, Binlog.class));
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
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

  public void process() {
    Thread runThread = new Thread(this);
    runThread.start();
  }

  protected void startRead(Binlog task) throws IOException {
    InputStream file = fileStorage.openReader(task.getPath());
    if (null == file) {
      logger.info("perhaps file of " + task.getPath() + " does not exist");
      return;
    }
    BinlogFileReader binlogFileReader =
      new BinlogFileReader(task, file, new DefaultEventListner.InnerEventListner(fileStorage));
    binlogFileReader.read();
  }

  static class KafkaProcessor extends TaskProcessor {

    @Override
    protected String consumeTask() {
      return super.consumeTask();
    }
  }
}
