package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.resolver.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static TaskRunner taskRunner;

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length == 0) return;
        try {
            taskRunner =
                    args[0].equalsIgnoreCase("dispense") ? TaskProcessor.defaultProcessor() : TaskProcessor.defaultProcessor();
            taskRunner.process();
        } catch (Exception e) {

            logger.error(e.getMessage(), e);
        }
    }
}
