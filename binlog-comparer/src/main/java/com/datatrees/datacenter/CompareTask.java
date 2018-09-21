package com.datatrees.datacenter;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.HiveCompare;
import com.datatrees.datacenter.compare.TiDBCompareFile;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import com.datatrees.datacenter.resolver.TaskProcessorListner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareTask implements TaskRunner {
    private static Logger LOG = LoggerFactory.getLogger(CompareTask.class);


    public void startCheck() {
        TaskProcessor.defaultProcessor()
                .setTopic(PropertiesUtility.defaultProperties().getProperty("queue.compare.topic"))
                .registerListner(
                        new TaskProcessorListner() {
                            @Override
                            public void onMessageReceived(String desc) {
                                BaseDataCompare TiDBCompare = new TiDBCompareFile();
                                LOG.info("start TiDB check...");
                                TiDBCompare.binLogCompare(desc, "update");
                                LOG.info("TiDB check finished");

                                BaseDataCompare hiveCompare = new HiveCompare();
                                LOG.info("start Hive check...");
                                hiveCompare.binLogCompare(desc, "update");
                            }
                        }).process();
    }

    @Override
    public void process() {
        startCheck();
    }
}
