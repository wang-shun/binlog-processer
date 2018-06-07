package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskRunner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class TransferTimerTask implements TaskRunner {
    private static final long INITIALDELAY = 0;
    private static final long PERIOD = 2;

    @Override
    public void process() {
        Runnable runnable = () -> {
            ServerTypeFactory factory = new ServerTypeFactory();
            BinlogFileTransfer binlogFileTransfer = factory.getServerType("AliBinLogFileTransfer");
            binlogFileTransfer.transfer();
        };
        ScheduledExecutorService service = Executors
                .newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 0, 5, TimeUnit.MINUTES);
    }

    public static void main(String[] args) {
        new TransferTimerTask().process();
    }
}
