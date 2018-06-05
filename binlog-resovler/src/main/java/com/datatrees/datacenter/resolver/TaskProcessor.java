package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.reader.BinlogFileReader;
import com.datatrees.datacenter.resolver.reader.DefaultEventListner;
import com.datatrees.datacenter.resolver.storage.HdfsStorage;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class TaskProcessor implements TaskRunner, Runnable {
    private static TaskProcessor __taskProcessor;
    private RBlockingQueue<String> blockingQueue;
    private static Logger logger = LoggerFactory.getLogger(TaskProcessor.class);

    private FileStorage fileStorage;
    private ExecutorService executorService = Executors.newFixedThreadPool(10, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        }
    });

    public static TaskProcessor defaultProcessor() {
        synchronized (TaskProcessor.class) {
            if (__taskProcessor == null) {
                Properties properties = PropertiesUtility.load("common.properties");
                String mode = properties.getProperty("queue.mode");
                switch (mode) {
                    case "default":
                        __taskProcessor = new TaskProcessor();
                        break;
                    default:
                        __taskProcessor = new TaskProcessor.KafkaProcessor();
                        break;
                }
            }
            return __taskProcessor;
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                    String taskDesc = consumeTask();
                    logger.info(String.format("start to read log file of %s", taskDesc));
                    if (StringUtils.isNotBlank(taskDesc)) {
                        executorService.submit(() -> {
                            try {
                                startRead(JSON.parseObject(taskDesc, Binlog.class));
                            } catch (IOException e) {
                                logger.error(e.getMessage(), e);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    TimeUnit.MILLISECONDS.sleep(1000L);
                } catch (InterruptedException e) {
                    logger.error(String.format("internal sleep failed."));
                }
            }
        }
    }

    protected TaskProcessor() {
        blockingQueue = RedisQueue.defaultQueue();
        fileStorage = new HdfsStorage();// TODO: 2018/6/1 reflect from config
    }

    protected String consumeTask() {
        return blockingQueue.poll();
    }

    public void process() {
        Thread runThread = new Thread(this);
        runThread.start();
    }

    private void startRead(Binlog task) throws IOException {
        BinlogFileReader binlogFileReader = new BinlogFileReader(task.getIdentity(), task.getInstanceId(),
                fileStorage.openReader(task.getPath()),
                new DefaultEventListner(fileStorage));
        binlogFileReader.read();
    }

    static class KafkaProcessor extends TaskProcessor {
        @Override
        protected String consumeTask() {
            return super.consumeTask();
        }
    }
}
