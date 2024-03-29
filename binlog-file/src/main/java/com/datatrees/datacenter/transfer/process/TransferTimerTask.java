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
public class TransferTimerTask implements TaskRunner {

    private static Logger LOG = LoggerFactory.getLogger(TransferTimerTask.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private long INITIAL_DELAY = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.initaildelay"));
    private long PERIOD = Integer.parseInt(properties.getProperty("AliBinLogFileTransfer.check.schedule.task.period"));
    private String REMOTE_SERVER = properties.getProperty("SERVER_TYPE", "idc");

    ProcessCheck processCheck;
    public static volatile Set processingSet = Collections.synchronizedSet(new HashSet<String>());
    public static volatile Map<String, Integer> processingMap = Collections.synchronizedMap(new HashMap<>());


    public TransferTimerTask() {
        processCheck = new ProcessCheck();
        processCheck.process();
    }

    @Override
    public void process() {
        Runnable runnable = () -> {
            try {
                ServerTypeFactory factory = new ServerTypeFactory();
                BinlogFileTransfer binlogFileTransfer;
                if ("aliyun".equals(REMOTE_SERVER.toLowerCase())) {
                    binlogFileTransfer = factory.getServerType("AliBinLogFileTransfer");
                } else {
                    binlogFileTransfer = factory.getServerType("LocalDataCenterTransfer");
                }
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
