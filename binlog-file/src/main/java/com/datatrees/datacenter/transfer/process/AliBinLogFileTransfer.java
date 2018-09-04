package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.process.threadmanager.TransferProcess;
import com.datatrees.datacenter.transfer.utility.BinLogFileUtil;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
    private Properties properties = PropertiesUtility.defaultProperties();
    private final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private final String HDFS_PATH = properties.getProperty("HDFS_PATH");
    private final long DOWN_TIME_INTER = Long.parseLong(properties.getProperty("DOWN_TIME_INTERVAL"));
    private int retryTimes = Integer.parseInt(properties.getProperty("process.check.schedule.task.retry"));
    private final long TIMESTAMP_DIFF = 8 * 60 * 60 * 1000L;
    private final long TIMEHOURS_DIFF = -8L;
    private long currentTime = System.currentTimeMillis() - TIMESTAMP_DIFF;
    private String startTime = TimeUtil.timeStamp2DateStr(currentTime - DOWN_TIME_INTER * 60 * 1000, TableInfo.UTC_FORMAT);
    private List<String> instanceIds = DBInstanceUtil.getAllPrimaryInstanceId();
    private String instanceStr = DBInstanceUtil.getInstancesString(instanceIds);
    private final String EXCLUDE_TIME_START = properties.getProperty("AliBinLogFileTransfer.schedule.task.exclude.start");
    private final String EXCLUDE_TIME_END = properties.getProperty("AliBinLogFileTransfer.schedule.task.exclude.end");
    private String endTime;
    private String dataBase = properties.getProperty("jdbc.database");

    @Override
    public void process() {

    }

    @Override
    public void transfer() {
        int currentHour = LocalDateTime.now().getHour();
        System.out.println("当前时间小时：" + currentHour);
        if (null!=EXCLUDE_TIME_START && null!=EXCLUDE_TIME_END) {
            int excludeStart = Integer.parseInt(EXCLUDE_TIME_START);
            int excludeEnd = Integer.parseInt(EXCLUDE_TIME_END);
            if (currentHour < excludeStart || currentHour > excludeEnd) {
                transferAll();
            }
        } else {
            transferAll();
        }
    }

    /**
     * 记录检查、未完成任务继续、新任务下载
     */
    private void transferAll() {
        // 创建API请求并设置参数
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setActionName(BINLOG_ACTION_NAME);
        // 检查下载记录和处理记录是否一致
        checkDataConsistency();
        //处理下载错误文件
        String fileSizeError = "select * from " + TableInfo.BINLOG_TRANS_TABLE + " where down_size is not null and file_size<>down_size";
        processErrorFile(fileSizeError);
        //处理解析错误文件
        String resolveError = "select * from " + TableInfo.BINLOG_PROC_TABLE + " where status<>1 and status<>0 and status<>7 and retry_times=" + retryTimes;
        processErrorFile(resolveError);
        //重新下载未完成的数据
        umCompleteProcess();
        //开始正常下载,将上一次的结束时间设置未这一次的开始时间
        binlogFilesRequest.setStartTime(startTime);
        LOG.info("the start time of download: " + startTime);
        endTime = TimeUtil.timeStamp2DateStr(currentTime, TableInfo.UTC_FORMAT);
        binlogFilesRequest.setEndTime(endTime);
        LOG.info("the end time of download: " + endTime);
        for (String instanceId : instanceIds) {
            instanceBinlogTrans(binlogFilesRequest, instanceId);
        }
    }

    /**
     * 检查下载表中是否有下载完但没有发往处理表中的数据
     */
    private void checkDataConsistency() {
        String sql = "select r.id,r.db_instance,r.file_name,r.bak_instance_id from " + TableInfo.BINLOG_TRANS_TABLE + " as r left join " + TableInfo.BINLOG_PROC_TABLE + " as p on r.db_instance=p.db_instance and r.file_name=p.file_name where p.id is null and r.status=1";
        List<Map<String, Object>> sendFailedRecord;
        try {
            sendFailedRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
            if (sendFailedRecord.size() > 0) {
                Iterator<Map<String, Object>> iterator = sendFailedRecord.iterator();
                Map<String, Object> recordMap;
                while (iterator.hasNext()) {

                    recordMap = iterator.next();
                    String dbInstance = String.valueOf(recordMap.get(TableInfo.DB_INSTANCE));
                    String fileName = String.valueOf(recordMap.get(TableInfo.FILE_NAME));
                    String backInstanceId = DBInstanceUtil.getBackInstanceId(dbInstance);
                    String path = HDFS_PATH + File.separator + dbInstance + File.separator + backInstanceId + File.separator + fileName;
                    String identity = dbInstance + "_"
                            + fileName;
                    String mysqlURL = DBInstanceUtil.getConnectString(dbInstance);
                    TaskDispensor.defaultDispensor().dispense(new Binlog(path, identity, mysqlURL));

                    Map<String, Object> map = new HashMap<>(5);
                    map.put(TableInfo.FILE_NAME, fileName);
                    map.put(TableInfo.DB_INSTANCE, dbInstance);
                    map.put(TableInfo.BAK_INSTANCE_ID, backInstanceId);
                    map.put(TableInfo.PROCESS_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                    try {
                        DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, map);
                    } catch (SQLException e) {
                        LOG.error("insert " + fileName + "to t_binlog_process failed");
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 检查是否有未下载完的数据并设置下次下载的开始时间
     */
    private void umCompleteProcess() {
        List<Map<String, Object>> unCompleteList = getUnCompleteTrans();
        if (unCompleteList.size() > 0) {
            reDownload(unCompleteList);
            Map<String, Object> lastTime = unCompleteList.get(unCompleteList.size() - 1);
            //设置下次下载的时间下载完成的结束时间
            Timestamp timestamp = (Timestamp) lastTime.get(TableInfo.DOWN_END_TIME);
            startTime = TimeUtil.dateToStr(Timestamp.valueOf(timestamp.toLocalDateTime().plusHours(TIMEHOURS_DIFF)), TableInfo.UTC_FORMAT);
        } else {
            try {
                StringBuilder endTimeSql = new StringBuilder();
                endTimeSql.append("select ")
                        .append(TableInfo.DOWN_END_TIME)
                        .append(" from  ")
                        .append(TableInfo.BINLOG_TRANS_TABLE)
                        .append(" where ")
                        .append(TableInfo.DB_INSTANCE)
                        .append(" in ")
                        .append("(")
                        .append(instanceStr)
                        .append(")")
                        .append(" order by ")
                        .append(TableInfo.DOWN_END_TIME)
                        .append(" desc limit 1");
                List<Map<String, Object>> lastDownEnd = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, endTimeSql.toString());
                if (!lastDownEnd.isEmpty()) {
                    //设置下载开始时间为上次结束时间
                    Timestamp timestamp = (Timestamp) lastDownEnd.get(0).get(TableInfo.DOWN_END_TIME);
                    startTime = TimeUtil.dateToStr(Timestamp.valueOf(timestamp.toLocalDateTime().plusHours(TIMEHOURS_DIFF)), TableInfo.UTC_FORMAT);
                }
            } catch (SQLException e) {
                LOG.error("can't get down_end from :" + TableInfo.BINLOG_TRANS_TABLE);
            }
        }
    }

    /**
     * 下载单个实例binlog文件
     *
     * @param binlogFilesRequest 服务请求
     * @param instanceId         实例
     */
    private void instanceBinlogTrans(DescribeBinlogFilesRequest binlogFilesRequest, String instanceId) {
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
                LOG.info("the batch_id of this download batch is :" + start + "_" + end);
                for (BinLogFile binLogFile : fileList) {
                    LOG.info("the real size of file [ " + binLogFile.getDownloadLink() + " ] is:" + binLogFile.getFileSize());

                    String hostInstanceId = binLogFile.getHostInstanceID();
                    String logEndTime = binLogFile.getLogEndTime();
                    String logStartTime = binLogFile.getLogBeginTime();
                    Long logEndTimeStamp = TimeUtil.utc2TimeStamp(logEndTime) + TIMESTAMP_DIFF;
                    String fileName = logEndTimeStamp / 1000 + "-" + BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN);

                    Map<String, Object> whereMap = new HashMap<>(4);
                    whereMap.put(TableInfo.DB_INSTANCE, instanceId);
                    whereMap.put(TableInfo.FILE_NAME, fileName);
                    try {
                        //判断这个binlog文件是否下载过
                        List<Map<String, Object>> recordCount = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_TRANS_TABLE, whereMap);
                        int count = recordCount.size();
                        if (count == 0) {
                            Map<String, Object> map = new HashMap<>(7);
                            map.put(TableInfo.FILE_SIZE, binLogFile.getFileSize());
                            map.put(TableInfo.DB_INSTANCE, instanceId);
                            map.put(TableInfo.BATCH_ID, batchId);
                            map.put(TableInfo.FILE_NAME, fileName);
                            map.put(TableInfo.BAK_INSTANCE_ID, hostInstanceId);
                            map.put(TableInfo.LOG_START_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logStartTime) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.LOG_END_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logEndTime) + TIMESTAMP_DIFF, TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.DOWN_LINK, binLogFile.getDownloadLink());
                            map.put(TableInfo.REQUEST_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                            map.put(TableInfo.HOST, IPUtility.ipAddress());
                            map.put(TableInfo.DOWN_START_TIME, start);
                            map.put(TableInfo.DOWN_END_TIME, end);
                            DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_TRANS_TABLE, map);
                        } else {
                            int status = (int) recordCount.get(0).get(TableInfo.DOWN_STATUS);
                            if (status == 1) {
                                continue;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    String dest = HDFS_PATH +
                            File.separator +
                            instanceId +
                            File.separator +
                            hostInstanceId;
                    TransInfo transInfo = new TransInfo(binLogFile.getDownloadLink(), dest,
                            fileName, instanceId);
                    boolean processed = TransferTimerTask.processingSet.contains(fileName);
                    if (!processed) {
                        TransferTimerTask.processingSet.add(fileName);
                        TransferProcess transferProcess = new TransferProcess(transInfo);
                        transferProcess.startTrans();
                        LOG.info("******************************");
                        LOG.info("the set size after add:" + TransferTimerTask.processingSet.size());
                        LOG.info("******************************");
                    }
                }
            } else {
                LOG.info("no binlog backed in the back instance : " + bakInstanceId);
            }
        } else {
            String beginTime = TimeUtil.timeStamp2DateStr(TimeUtil.strToDate(binlogFilesRequest.getStartTime(), TableInfo.UTC_FORMAT).getTime() + TIMESTAMP_DIFF, TableInfo.UTC_FORMAT);
            String finishTime = TimeUtil.timeStamp2DateStr(TimeUtil.strToDate(binlogFilesRequest.getEndTime(), TableInfo.UTC_FORMAT).getTime() + TIMESTAMP_DIFF, TableInfo.UTC_FORMAT);
            LOG.info("no binlog find in the : " + instanceId + " with time between " + beginTime + " and " + finishTime);
        }
    }

    /**
     * 返回为传输未完成的binlog文件信息
     */
    private List<Map<String, Object>> getUnCompleteTrans() {
        List<Map<String, Object>> resultList = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("select")
                    .append(" ")
                    .append("distinct")
                    .append(" ")
                    .append(TableInfo.DOWN_START_TIME)
                    .append(",")
                    .append(TableInfo.DOWN_END_TIME)
                    .append(" ")
                    .append("from")
                    .append(" ")
                    .append(TableInfo.BINLOG_TRANS_TABLE)
                    .append(" ")
                    .append("where")
                    .append(" ")
                    .append(TableInfo.DOWN_STATUS)
                    .append("=")
                    .append(DownloadStatus.UNCOMPLETED.getValue())
                    .append(" ")
                    .append("and")
                    .append(" ")
                    .append(TableInfo.DB_INSTANCE)
                    .append(" ")
                    .append("in")
                    .append(" ")
                    .append("(")
                    .append(instanceStr)
                    .append(")")
                    .append(" ")
                    .append("order by")
                    .append(" ")
                    .append(TableInfo.DOWN_START_TIME);

            resultList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return resultList;
    }


    /**
     * 处理未下载完成的的binlog文件
     */
    private void reDownload(List<Map<String, Object>> uncompleted) {
        for (Map<String, Object> info : uncompleted) {
            Timestamp startTs = (Timestamp) info.get(TableInfo.DOWN_START_TIME);
            Timestamp endTs = (Timestamp) info.get(TableInfo.DOWN_END_TIME);
            LocalDateTime startLDT = startTs.toLocalDateTime().plusHours(TIMEHOURS_DIFF);
            LocalDateTime endLDT = endTs.toLocalDateTime().plusHours(TIMEHOURS_DIFF);

            startTime = TimeUtil.dateToStr(Timestamp.valueOf(startLDT), TableInfo.UTC_FORMAT);
            endTime = TimeUtil.dateToStr(Timestamp.valueOf(endLDT), TableInfo.UTC_FORMAT);
            DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
            binlogFilesRequest.setStartTime(startTime);
            binlogFilesRequest.setEndTime(endTime);
            for (String instanceId : instanceIds) {
                instanceBinlogTrans(binlogFilesRequest, instanceId);
            }
        }
    }

    /**
     * 对解析或者下载错误的文件重新下载
     */
    private void processErrorFile(String sql) {
        try {
            List<Map<String, Object>> errorData = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
            if (!errorData.isEmpty()) {
                for (Map<String, Object> objectMap : errorData) {
                    String fileName = (String) objectMap.get(TableInfo.FILE_NAME);
                    String dbInstance = (String) objectMap.get(TableInfo.DB_INSTANCE);
                    String bakInstanceId = (String) objectMap.get(TableInfo.BAK_INSTANCE_ID);
                    String path = HDFS_PATH + File.separator + dbInstance + File.separator + bakInstanceId + File.separator + fileName;
                    boolean status = HDFSFileUtility.checkAndDel(path);
                    boolean fileExists = HDFSFileUtility.getFileSystem(path).exists(new Path(path));
                    if (status||fileExists) {
                        LOG.info("delete file: " + fileName + " from HDFS success");
                        Map<String, Object> whereMap = new HashMap<>();
                        whereMap.put(TableInfo.FILE_NAME, fileName);
                        whereMap.put(TableInfo.DB_INSTANCE, dbInstance);
                        DBUtil.delete(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, whereMap);
                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put(TableInfo.DOWN_STATUS, 0);
                        valueMap.put(TableInfo.DOWN_SIZE, null);
                        valueMap.put(TableInfo.REQUEST_END, null);
                        DBUtil.update(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_TRANS_TABLE, valueMap, whereMap);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getSQLState(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
        }
    }
}