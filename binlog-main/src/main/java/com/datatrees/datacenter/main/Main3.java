package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.ReflectUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main3 {

    private static TaskRunner taskRunner;

    private static Logger logger = LoggerFactory.getLogger(Main3.class);

    public static void main(String[] args) {
        RBlockingQueue<String> queue = RedisQueue.defaultQueue();
        try {
            Properties value = PropertiesUtility.defaultProperties();
            if (value.getProperty("runner.class") == null) {
                taskRunner = TaskProcessor.defaultProcessor();
            } else {
                taskRunner = ReflectUtility.<TaskRunner>reflect(value.getProperty("runner.class"));
            }
            taskRunner.process();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
