package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class TransferTimerTask implements TaskRunner {

    private static final Properties properties = PropertiesUtility.defaultProperties();
    private static final long INITIAL_DELAY = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.initaildelay"));
    private static final long PERIOD = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.period"));

    ProcessCheck processCheck;

    public TransferTimerTask() {
        processCheck = new ProcessCheck();
        processCheck.process();
    }

    @Override
    public void process() {
        Runnable runnable = () -> {
            ServerTypeFactory factory = new ServerTypeFactory();
            BinlogFileTransfer binlogFileTransfer = factory.getServerType("AliBinLogFileTransfer");
            binlogFileTransfer.transfer();
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, INITIAL_DELAY, PERIOD, TimeUnit.MINUTES);
    }
}
