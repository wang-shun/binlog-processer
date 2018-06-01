package com.datatrees.datacenter.resolver;

import com.alibaba.fastjson.JSON;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.RedisQueue;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.reader.BinlogFileReader;
import com.datatrees.datacenter.resolver.reader.DefaultEventConsumer;
import com.datatrees.datacenter.resolver.storage.HdfsStorage;
import javafx.concurrent.Task;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class TaskProcessor {
    private static TaskProcessor __taskProcessor;
    private RBlockingQueue<String> blockingQueue;
    private static Logger logger = LoggerFactory.getLogger(TaskProcessor.class);

    private FileStorage fileStorage;
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        }
    });

    protected TaskProcessor() {
        blockingQueue = RedisQueue.defaultQueue();
        fileStorage = new HdfsStorage();// TODO: 2018/6/1 reflect from config
    }

    protected String consumeTask() {
        return blockingQueue.poll();
    }

    public void process() {
        executorService.submit(() -> {
            do {
                if (blockingQueue.isEmpty()) continue;
                String task = consumeTask();
                if (StringUtils.isBlank(task)) continue;

                try {
                    try {
                        TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    }

                    Binlog binlog = JSON.parseObject(task, Binlog.class);
                    BinlogFileReader binlogFileReader = new BinlogFileReader(binlog.getIdentity(), binlog.getInstanceId(), fileStorage.openReader(binlog.getPath()),
                            new DefaultEventConsumer(fileStorage));
                    binlogFileReader.read();

                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    continue;
                }

            } while (true);
        });
    }

    class KafkaProcessor extends TaskProcessor
    {
        @Override
        protected String consumeTask() {
            return super.consumeTask();
        }
    }
}
