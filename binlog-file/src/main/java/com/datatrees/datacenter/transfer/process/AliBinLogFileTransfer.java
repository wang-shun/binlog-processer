package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.IPUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.process.threadmanager.TransferProcess;
import com.datatrees.datacenter.transfer.utility.BinLogFileUtil;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import com.datatrees.datacenter.transfer.utility.FileUtil;
import com.datatrees.datacenter.transfer.utility.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 根据条件下载指定实例binlog文件
 *
 * @author personalc
 */
public class AliBinLogFileTransfer implements TaskRunner, BinlogFileTransfer {
    private static Logger LOG = LoggerFactory.getLogger(AliBinLogFileTransfer.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private static final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private static final String HDFS_PATH = properties.getProperty("HDFS_PATH");
    private static final long DOWN_TIME_INTER = Long.parseLong(properties.getProperty("DOWN_TIME_INTERVAL"));
    private static final String BINLOG_TRANS_TABLE = TableInfo.BINLOG_TRANS_TABLE;
    private static final long TIMESTAMP_DIFF = 8 * 60 * 60 * 1000L;
    private static final long TIMEHOURS_DIFF = -8L;
    private long currentTime = System.currentTimeMillis() - TIMESTAMP_DIFF;
    private String startTime = TimeUtil.timeStamp2DateStr(currentTime - DOWN_TIME_INTER * 60 * 1000, TableInfo.UTC_FORMAT);
    private String endTime;
    private static FileUtil fileUtil = new FileUtil();
    List<DBInstance> instances = DBInstanceUtil.getAllPrimaryDBInstance();


    /**
     * 下载单个实例binlog文件
     *
     * @param binlogFilesRequest 服务请求
     * @param dbInstance         实例
     */
    private void instanceBinlogTrans(DescribeBinlogFilesRequest binlogFilesRequest, DBInstance dbInstance) {
        String instanceId = dbInstance.getDBInstanceId();
        binlogFilesRequest.setDBInstanceId(instanceId);
        List<BinLogFile> binLogFiles = BinLogFileUtil.getBinLogFiles(AliYunConfig.getClient(), binlogFilesRequest, AliYunConfig.getProfile());
        if (binLogFiles.size() > 0) {
            String bakInstanceId = DBInstanceUtil.getBackInstanceId(instanceId);
            List<BinLogFile> fileList = binLogFiles.parallelStream()
                    .filter(binLogFile -> binLogFile.getHostInstanceID().equals(bakInstanceId)).collect(Collectors.toList());
            long fileListSize = fileList.size();
            if (fileListSize > 0) {
                String start = TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(binlogFilesRequest.getStartTime()) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT);
                String end = TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(binlogFilesRequest.getEndTime()) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT);
                String batchId = start + "_" + end;
                LOG.info("the batch_id of this download batch is :" + binlogFilesRequest.getStartTime() + "_" + binlogFilesRequest.getEndTime());
                for (BinLogFile binLogFile : fileList) {
                    LOG.info("the real size of file [ " + binLogFile.getDownloadLink() + " ] is:" + binLogFile.getFileSize());
                    String dbInstanceId = dbInstance.getDBInstanceId();
                    String hostInstanceId = binLogFile.getHostInstanceID();

                    String logEndTime = binLogFile.getLogEndTime();
                    String logStartTime = binLogFile.getLogBeginTime();

                    Long logEndTimeStamp = TimeUtil.utc2TimeStamp(logEndTime) + TIMESTAMP_DIFF;
                    String fileName = logEndTimeStamp / 1000 + "-" + BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN);
                    Map<String, Object> whereMap = new HashMap<>(4);
                    whereMap.put(TableInfo.DB_INSTANCE, dbInstanceId);
                    whereMap.put(TableInfo.FILE_NAME, fileName);
                    try {
                        //判断这个binlog文件是否下载过
                        int recordCount = DBUtil.query(BINLOG_TRANS_TABLE, whereMap).size();
                        if (recordCount == 0) {
                            Map<String, Object> map = new HashMap<>(7);
                            map.put(TableInfo.FILE_SIZE, fileUtil.getFileSize(binLogFile.getDownloadLink()));
                            map.put(TableInfo.DB_INSTANCE, dbInstanceId);
                            map.put(TableInfo.BATCH_ID, batchId);
                            map.put(TableInfo.FILE_NAME, fileName);
                            map.put(TableInfo.BAK_INSTANCE_ID, hostInstanceId);
                            map.put(TableInfo.LOG_START_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logStartTime) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.LOG_END_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logEndTime) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.DOWN_LINK, binLogFile.getDownloadLink());
                            map.put(TableInfo.REQUEST_START, TimeUtil.timeStamp2DateStr(System.currentTimeMillis(), TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.HOST, IPUtility.ipAddress());
                            map.put(TableInfo.DOWN_START_TIME, start);
                            map.put(TableInfo.DOWN_END_TIME, end);
                            DBUtil.insert(BINLOG_TRANS_TABLE, map);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    String filePath = HDFS_PATH +
                            File.separator +
                            dbInstanceId +
                            File.separator +
                            hostInstanceId;
                    TransInfo transInfo = new TransInfo(binLogFile.getDownloadLink(), filePath,
                            fileName, dbInstance);
                    TransferProcess transferProcess = new TransferProcess(transInfo);
                    transferProcess.startTrans();
                }
            } else {
                LOG.info("no binlong backed in the master instance");
            }
        } else {
            LOG.info("no binlog find in the: " + instanceId + " with time between " + binlogFilesRequest.getStartTime() + " and " + binlogFilesRequest.getEndTime());
        }
    }

    /**
     * 返回为传输未完成的binlog文件信息
     */
    private List<Map<String, Object>> getUnCompleteTrans() {
        List<Map<String, Object>> resultList = null;
        try {
            resultList = DBUtil.query(BINLOG_TRANS_TABLE, true, new String[]{TableInfo.DOWN_START_TIME, TableInfo.DOWN_END_TIME}, TableInfo.DOWN_STATUS + "=" + DownloadStatus.UNCOMPLETED.getValue(), null, null, null, TableInfo.DOWN_START_TIME, null);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return resultList;
    }

    /**
     * 处理未下载完成的的binlog文件
     */
    private void processUnComplete(List<Map<String, Object>> uncompleted) {
        Iterator itr = uncompleted.iterator();
        while (itr.hasNext()) {
            Map<String, Object> info = (Map<String, Object>) itr.next();
            Timestamp startTs = (Timestamp) info.get(TableInfo.DOWN_START_TIME);
            Timestamp endTs = (Timestamp) info.get(TableInfo.DOWN_END_TIME);
            LocalDateTime startLDT = startTs.toLocalDateTime().plusHours(TIMEHOURS_DIFF);
            LocalDateTime endLDT = endTs.toLocalDateTime().plusHours(TIMEHOURS_DIFF);

            startTime = TimeUtil.dateToStr(Timestamp.valueOf(startLDT), TableInfo.UTC_FORMAT);
            endTime = TimeUtil.dateToStr(Timestamp.valueOf(endLDT), TableInfo.UTC_FORMAT);
            DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
            binlogFilesRequest.setStartTime(startTime);
            binlogFilesRequest.setEndTime(endTime);
            for (DBInstance dbInstance : instances) {
                instanceBinlogTrans(binlogFilesRequest, dbInstance);
            }
        }
    }


    @Override
    public void process() {

    }

    @Override
    public void transfer() {
        // 创建API请求并设置参数
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setActionName(BINLOG_ACTION_NAME);
        // 检查下载记录和处理记录是否一致
        String sql = "select r.id,r.db_instance,r.file_name,r.bak_instance_id from " + TableInfo.BINLOG_TRANS_TABLE + " as r left join " + TableInfo.BINLOG_PROC_TABLE + " as p on r.db_instance=p.db_instance and r.file_name=p.file_name where p.id is null and r.status=1";
        List<Map<String, Object>> sendFailedRecord;
        try {
            sendFailedRecord = DBUtil.query(sql);
            if (sendFailedRecord.size() > 0) {
                Iterator<Map<String, Object>> iterator = sendFailedRecord.iterator();
                Map<String, Object> recordMap;
                while (iterator.hasNext()) {
                    recordMap = iterator.next();
                    String path = HDFS_PATH + File.separator + recordMap.get(TableInfo.DB_INSTANCE) + File.separator + recordMap.get(TableInfo.BAK_INSTANCE_ID) + File.separator + recordMap.get(TableInfo.FILE_NAME);
                    String identity = recordMap.get(TableInfo.DB_INSTANCE) + "_"
                            + recordMap.get(TableInfo.FILE_NAME);
                    String mysqlURL = DBInstanceUtil.getConnectString((String) recordMap.get(TableInfo.DB_INSTANCE));
                    TaskDispensor.defaultDispensor().dispense(new Binlog(path, identity, mysqlURL));
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        //重新下载未完成的数据
        List<Map<String, Object>> unCompleteList = getUnCompleteTrans();
        if (unCompleteList.size() > 0) {
            processUnComplete(unCompleteList);
            Map<String, Object> lastTime = unCompleteList.get(unCompleteList.size() - 1);
            //设置下次下载的时间下载完成的结束时间
            Timestamp timestamp = (Timestamp) lastTime.get(TableInfo.DOWN_END_TIME);
            startTime = TimeUtil.dateToStr(Timestamp.valueOf(timestamp.toLocalDateTime().plusHours(TIMEHOURS_DIFF)), TableInfo.UTC_FORMAT);
        } else {
            try {
                List<Map<String, Object>> lastDownEnd = DBUtil.query("select " + TableInfo.DOWN_END_TIME + " from " + TableInfo.BINLOG_TRANS_TABLE + " order by " + TableInfo.DOWN_END_TIME + " desc limit 1");
                if (lastDownEnd.size() > 0) {
                    //设置下载开始时间为上次结束时间

                    Timestamp timestamp = (Timestamp) lastDownEnd.get(0).get(TableInfo.DOWN_END_TIME);
                    startTime = TimeUtil.dateToStr(Timestamp.valueOf(timestamp.toLocalDateTime().plusHours(TIMEHOURS_DIFF)), TableInfo.UTC_FORMAT);
                }
            } catch (SQLException e) {
                LOG.error("can't get down_end from :" + TableInfo.BINLOG_TRANS_TABLE);
            }
        }
        //开始正常下载,将上一次的结束时间设置未这一次的开始时间
        binlogFilesRequest.setStartTime(startTime);
        LOG.info("the start time of download: " + startTime);
        endTime = TimeUtil.timeStamp2DateStr(currentTime, TableInfo.UTC_FORMAT);
        binlogFilesRequest.setEndTime(endTime);
        LOG.info("the end time of download: " + endTime);
        for (DBInstance dbInstance : instances) {
            instanceBinlogTrans(binlogFilesRequest, dbInstance);
        }
    }
}