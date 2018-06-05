package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.transfer.bean.DownLoadTable;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.core.utility.DBUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author personalc
 */
public class ProcessCheck {
    public static void main(String[] args) {
        Runnable runnable = () -> {
            Map<String, Object> whereMap = new HashMap<>(1);
            List<Map<String, Object>> resultList = null;
            whereMap.put(DownLoadTable.DOWN_STATUS, DownloadStatus.UNCOMPLETED.getValue());
            try {
                resultList = DBUtil.query(DownLoadTable.BINLOG_PROC_TABLE, whereMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // TODO: 2018/5/31 add send to kafka
            // task to run goes here
            System.out.println("Hello !!");
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 10, 10, TimeUnit.SECONDS);
    }
}