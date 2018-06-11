package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatrees.datacenter.core.utility.ReflectUtility;

import java.util.Properties;

public class Main {

    private static TaskRunner taskRunner;

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
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
