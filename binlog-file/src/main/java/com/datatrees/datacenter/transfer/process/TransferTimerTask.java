package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class TransferTimerTask implements TaskRunner {

    private static Logger LOG = LoggerFactory.getLogger(TransferTimerTask.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private long INITIAL_DELAY = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.initaildelay"));
    private long PERIOD = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.period"));

    ProcessCheck processCheck;
    public  static Set<String> processingSet;

    public TransferTimerTask() {
            processCheck = new ProcessCheck();
            processCheck.process();
    }

    @Override
    public void process() {
        processingSet=new HashSet<>();
        Runnable runnable = () -> {
            try {
                ServerTypeFactory factory = new ServerTypeFactory();
                BinlogFileTransfer binlogFileTransfer = factory.getServerType("AliBinLogFileTransfer");
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
