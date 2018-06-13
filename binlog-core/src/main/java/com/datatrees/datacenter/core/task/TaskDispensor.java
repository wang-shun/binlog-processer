package com.datatrees.datacenter.core.task;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.ReflectUtility;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskDispensor {

  protected static Logger logger = LoggerFactory.getLogger(TaskDispensor.class);
  protected static Properties __properties;
  protected static ExecutorService executorService = Executors.newFixedThreadPool(100);
  private static TaskDispensor __taskDispensor;

  protected TaskDispensor() {
    __properties = PropertiesUtility.defaultProperties();
    }

  public static TaskDispensor defaultDispensor() {
    synchronized (TaskDispensor.class) {
      if (__taskDispensor == null) {
        Properties properties = PropertiesUtility.defaultProperties();
        String mode = properties.getProperty("queue.dispense.class");
        __taskDispensor = ReflectUtility.reflect(mode);
        if (__taskDispensor == null) {
          __taskDispensor = new RabbitMqDispensor();
        }
      }
      return __taskDispensor;
    }
  }

  public void dispense(Binlog binlog) {
    try {
      RedisQueue.defaultQueue().offer(JSON.toJSONString(binlog));
      logger.info(String.format("success to offer binlog of %s", binlog.toString()));
    } catch (Exception e) {
      throw new BinlogException(
        String.format("error to offer binlog of %s", binlog.toString()), e
      );
    }
  }

  static class KafkaDispensor extends TaskDispensor {

    @Override
    public void dispense(Binlog binlog) {
      KafkaProducer producer = new KafkaProducer();
      producer.send(JSON.toJSONString(binlog));
    }
  }
}
