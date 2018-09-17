package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.check.ProcessCheck;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class TransferTimerTaskCopy implements TaskRunner {

    private static Logger LOG = LoggerFactory.getLogger(TransferTimerTaskCopy.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private long INITIAL_DELAY = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.initaildelay"));
    private long PERIOD = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.period"));
    private String REMOTE_SERVER = properties.getProperty("SERVER_TYPE", "idc");
    public  static volatile Map<String,Integer> processingMap = new HashMap<>();

    public TransferTimerTaskCopy() {
    }

    @Override
    public void process() {
        Runnable runnable = () -> {
            try {
                ServerTypeFactory factory = new ServerTypeFactory();
                BinlogFileTransfer binlogFileTransfer = factory.getServerType("LocalDataCenterTransfer");
                binlogFileTransfer.transfer();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, INITIAL_DELAY, PERIOD, TimeUnit.MINUTES);
        //service.scheduleAtFixedRate(runnable,(INITIAL_DELAY+1)*1000,PERIOD*10,TimeUnit.MINUTES);
    }
}
