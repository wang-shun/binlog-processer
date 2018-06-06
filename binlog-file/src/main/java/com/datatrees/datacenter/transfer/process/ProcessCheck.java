package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.core.utility.DBUtil;
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
    private static final String DEST = properties.getProperty("HDFS_PATH");

    public static void main(String[] args) {
        Runnable runnable = () -> {
            Map<String, Object> whereMap = new HashMap<>(1);
            List<Map<String, Object>> resultList = null;
            whereMap.put(TableInfo.DOWN_STATUS, DownloadStatus.UNCOMPLETED.getValue());
            Map<String, Object> oneRecord;
            try {
                resultList = DBUtil.query(TableInfo.BINLOG_PROC_TABLE, whereMap);
                if (resultList.size() > 0) {
                    Iterator<Map<String, Object>> iterator = resultList.iterator();
                    while (iterator.hasNext()) {
                        oneRecord = iterator.next();
                        TaskDispensor.defaultDispensor().dispense(
                                new Binlog(DEST + File.separator + oneRecord.get(TableInfo.FILE_NAME),
                                        oneRecord.get(TableInfo.DB_INSTANCE) + "_"
                                                + oneRecord.get(TableInfo.FILE_NAME),
                                        DBInstanceUtil.getConnectString((String) oneRecord.get(TableInfo.DB_INSTANCE))));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 10, 10, TimeUnit.SECONDS);
    }
}