package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import com.datatrees.datacenter.transfer.utility.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author personalc
 */
public class ProcessCheck {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessCheck.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private String DEST = properties != null ? properties.getProperty("HDFS_PATH") : null;
    private int interval = Integer.parseInt(properties.getProperty("process.check.interval"));
    private String TIME_SCALE = properties.getProperty("process.check.time.scale");
    private long INITIAL_DELAY = Integer.parseInt(properties.getProperty("process.check.schedule.task.initaildelay"));
    private long THREAD_PERIOD = Integer.parseInt(properties.getProperty("process.check.schedule.task.period"));
    private int RETRY_TIMES = Integer.parseInt(properties.getProperty("process.check.schedule.task.retry"));
    private List<String> instanceIds = DBInstanceUtil.getAllPrimaryInstanceId();
    private String instanceStr = DBInstanceUtil.getInstancesString(instanceIds);

    public void process() {
        Runnable runnable = () -> {
            try {
                LOG.info("start process check....");
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
                            .append(TableInfo.DB_INSTANCE)
                            .append(" ")
                            .append("in")
                            .append(" ")
                            .append("(" + instanceStr + ")")
                            .append(" ")
                            .append(" and ")
                            .append(" ")
                            .append(TableInfo.PROCESS_STATUS)
                            .append("=0")
                            .append(" ")
                            .append(" and ")
                            .append(" ")
                            .append(TableInfo.RETRY_TIMES)
                            .append(" ")
                            .append("<")
                            .append(RETRY_TIMES)
                            .append(" ")
                            .append("and")
                            .append(" ")
                            .append(TableInfo.PROCESS_START)
                            .append("<")
                            .append("now()-interval")
                            .append(" ")
                            .append(interval)
                            .append(" ")
                            .append(TIME_SCALE);

                    resultList = DBUtil.query(sql.toString());
                    if (resultList.size() > 0) {
                        Iterator<Map<String, Object>> iterator = resultList.iterator();
                        while (iterator.hasNext()) {
                            oneRecord = iterator.next();
                            String instanceId = String.valueOf(oneRecord.get(TableInfo.DB_INSTANCE));
                            String fileName = String.valueOf(oneRecord.get(TableInfo.FILE_NAME));
                            String bakInstanceId = String.valueOf(oneRecord.get(TableInfo.BAK_INSTANCE_ID));
                            // send to kafka
                            String filePath = DEST + File.separator + instanceId + File.separator + bakInstanceId + File.separator + fileName;
                            String identity = instanceId + "_" + fileName;
                            String mysqlURL = DBInstanceUtil.getConnectString((String) oneRecord.get(TableInfo.DB_INSTANCE));
                            TaskDispensor.defaultDispensor().dispense(new Binlog(filePath, identity, mysqlURL));
                            LOG.info("send " + identity + " to massage queue");

                            //update t_binlog_process table
                            int retryTimes = (Integer) oneRecord.get(TableInfo.RETRY_TIMES) + 1;
                            Map<String, Object> whereMap = new HashMap<>(2);
                            whereMap.put(TableInfo.DB_INSTANCE, instanceId);
                            whereMap.put(TableInfo.FILE_NAME, fileName);
                            Map<String, Object> valueMap = new HashMap<>(2);
                            valueMap.put(TableInfo.RETRY_TIMES, retryTimes);
                            Date process_start=TimeUtil.stampToDate(System.currentTimeMillis());
                            valueMap.put(TableInfo.PROCESS_START,process_start);
                            DBUtil.update(TableInfo.BINLOG_PROC_TABLE, valueMap, whereMap);
                            LOG.info("update t_binlog_process table, set " + identity + " retry: " + retryTimes + " and process_start: "+process_start);
                        }
                    }
                } catch (Exception e) {
                    LOG.info("something error with queue or database");
                    e.printStackTrace();
                }
            }catch (Exception e) {
                LOG.error(e.getMessage(),e);
            }
        };

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, INITIAL_DELAY, THREAD_PERIOD, TimeUnit.MINUTES);
    }
}