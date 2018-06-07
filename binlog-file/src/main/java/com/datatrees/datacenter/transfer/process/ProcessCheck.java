package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.bean.SendStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author personalc
 */
public class ProcessCheck {

    private static Properties properties = PropertiesUtility.load("instance.properties");
    private static String DEST = properties != null ? properties.getProperty("HDFS_PATH") : null;
    private static int interval = 100;
    private static final long INITIALDELAY = 10;
    private static final long THREAD_PERIOD = 10;

    public static void main(String[] args) {
        Runnable runnable = () -> {
            List<Map<String, Object>> resultList;
            Map<String, Object> oneRecord;
            try {
                StringBuilder sql = new StringBuilder();
                sql.append("select * from ")
                        .append(" ")
                        .append(TableInfo.BINLOG_PROC_TABLE)
                        .append(" ")
                        .append("where")
                        .append(" ")
                        .append(TableInfo.PROCESS_START)
                        .append("<")
                        .append("curdate()-interval")
                        .append(" ")
                        .append(interval)
                        .append(" ")
                        .append("hour")
                        .append(" ")
                        .append("and")
                        .append(" ")
                        .append(TableInfo.PROCESS_STATUS)
                        .append(" ")
                        .append("=")
                        .append(SendStatus.NO.getValue());
                resultList = DBUtil.query(sql.toString());

                if (resultList.size() > 0) {
                    Iterator<Map<String, Object>> iterator = resultList.iterator();
                    while (iterator.hasNext()) {
                        oneRecord = iterator.next();
                        String instanceId = String.valueOf(oneRecord.get(TableInfo.DB_INSTANCE));
                        String fileName = String.valueOf(oneRecord.get(TableInfo.FILE_NAME));
                        String bakInstanceId = String.valueOf(oneRecord.get(TableInfo.BAK_INSTANCE_ID));
                        int retryTimes = (Integer) oneRecord.get(TableInfo.RETRY_TIMES);

                        // send to kafka
                        TaskDispensor.defaultDispensor().dispense(
                                new Binlog(DEST + File.separator + bakInstanceId + File.separator + fileName,
                                        instanceId + "_"
                                                + fileName,
                                        DBInstanceUtil.getConnectString((String) oneRecord.get(TableInfo.DB_INSTANCE))));

                        //update t_binlog_process table
                        Map<String, Object> whereMap = new HashMap<>(2);
                        whereMap.put(TableInfo.DB_INSTANCE, instanceId);
                        whereMap.put(TableInfo.FILE_NAME, fileName);
                        Map<String, Object> valueMap = new HashMap<>(1);
                        valueMap.put(TableInfo.RETRY_TIMES, retryTimes + 1);
                        DBUtil.update(TableInfo.BINLOG_PROC_TABLE, valueMap, whereMap);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, INITIALDELAY, THREAD_PERIOD, TimeUnit.SECONDS);
    }
}