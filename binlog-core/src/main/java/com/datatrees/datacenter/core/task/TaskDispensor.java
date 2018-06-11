package com.datatrees.datacenter.core.task;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.Properties;

public class TaskDispensor {

    private static TaskDispensor __taskDispensor;

    protected TaskDispensor() {
//        TaskDispensor.defaultDispensor().dispense();
    }

    public static TaskDispensor defaultDispensor() {
        synchronized (TaskDispensor.class) {
            if (__taskDispensor == null) {
                Properties properties = PropertiesUtility.defaultProperties();
                String mode = properties.getProperty("queue.mode");
                switch (mode) {
                    case "default":
                        __taskDispensor = new TaskDispensor();
                        break;
                    default:
                        __taskDispensor = new KafkaDispensor();
                        break;
                }

            }
            return __taskDispensor;
        }
    }

    public void dispense(Binlog binlog) {
        RedisQueue.defaultQueue().offer(JSON.toJSONString(binlog));
    }

    static class KafkaDispensor extends TaskDispensor {
        @Override
        public void dispense(Binlog binlog) {
            KafkaProducer producer = new KafkaProducer();
            producer.send(JSON.toJSONString(binlog));
        }
    }
}
