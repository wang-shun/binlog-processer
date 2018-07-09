package com.datatrees.datacenter;

import com.datatrees.datacenter.compare.DataCompare;
import com.datatrees.datacenter.compare.TiDBCompare;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.resolver.TaskProcessor;
import com.datatrees.datacenter.resolver.TaskProcessorListner;

public class compareTask implements TaskRunner {
    public void startCheck() {
        TaskProcessor.defaultProcessor().setTopic("local_topic").registerListner(
                new TaskProcessorListner() {
                    @Override
                    public void onMessageReceived(String desc) {
                        DataCompare compare = new TiDBCompare();
                        // TODO: 2018/7/9  
                        String src = "";
                        String dest = "";
                        compare.binLogCompare(src, dest);

                    }
                }).process();
    }

    @Override
    public void process() {

    }
}
