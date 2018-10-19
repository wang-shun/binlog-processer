package com.datatrees.datacenter;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.HiveCompareByFile;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import com.datatrees.datacenter.resolver.TaskProcessorListner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareTask implements TaskRunner {
    private static Logger LOG = LoggerFactory.getLogger(CompareTask.class);
    private final String partitionType = PropertiesUtility.defaultProperties().getProperty("partition.type");
    BaseDataCompare hiveCompare = new HiveCompareByFile();

    public void startCheck() {
        TaskProcessor.defaultProcessor()
                .setTopic(PropertiesUtility.defaultProperties().getProperty("queue.compare.topic"))
                .registerListner(
                        new TaskProcessorListner() {
                            @Override
                            public void onMessageReceived(String desc) {
                                LOG.info("start Hive check...");
                                hiveCompare.binLogCompare(desc, partitionType);
                                LOG.info("Hive check finished");
                            }
                        }).process();
    }

    @Override
    public void process() {
        startCheck();
    }
}
