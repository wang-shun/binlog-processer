package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PrometheusMetrics;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.ReflectUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main4 {
    private static TaskRunner taskRunner;

    private static PrometheusMetrics prometheusMetrics = new PrometheusMetrics();
    private static Logger logger = LoggerFactory.getLogger(BinlogMain.class);
    public static void main(String[] args) {
        try {
            Properties value = PropertiesUtility.defaultProperties();
            if (value.getProperty("runner.class") == null) {
                taskRunner = TaskProcessor.defaultProcessor();
            } else {
                taskRunner = ReflectUtility.<TaskRunner>reflect("com.datatrees.datacenter.transfer.process.TransferTimerTaskCopy");
            }
            taskRunner.process();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
