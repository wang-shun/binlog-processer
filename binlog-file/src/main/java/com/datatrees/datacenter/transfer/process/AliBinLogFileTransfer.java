package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.IPUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.process.thread.TransferProcess;
import com.datatrees.datacenter.transfer.utility.BinLogFileUtil;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 根据条件下载指定实例binlog文件
 *
 * @author personalc
 */
public class AliBinLogFileTransfer extends BinlogFileTransfer  {
    private static Logger LOG = LoggerFactory.getLogger(AliBinLogFileTransfer.class);
    private final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private final String BINLOG_ACTION_NAME = "DescribeBinlogFiles";
    private final long DOWN_TIME_INTER = Long.parseLong(properties.getProperty("DOWN_TIME_INTERVAL"));
    private final String EXCLUDE_TIME_START = properties.getProperty("AliBinLogFileTransfer.schedule.task.exclude.start");
    private final String EXCLUDE_TIME_END = properties.getProperty("AliBinLogFileTransfer.schedule.task.exclude.end");
    private final long TIMESTAMP_DIFF = 8 * 60 * 60 * 1000L;
    private final long TIMEHOURS_DIFF = -8L;
    private long currentTime = System.currentTimeMillis() - TIMESTAMP_DIFF;
    private String startTime = TimeUtil.timeStamp2DateStr(currentTime - DOWN_TIME_INTER * 60 * 1000, TableInfo.UTC_FORMAT);
    private List<String> instanceIds = DBInstanceUtil.getAllPrimaryInstanceId();
    private String instanceStr = DBInstanceUtil.getInstancesString(instanceIds);
    private String endTime;


    @Override
    public void process() {

    }


    @Override
    public void transfer() {
        int currentHour = LocalDateTime.now().getHour();
        System.out.println("the hours of current ：" + currentHour);
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
        umCompleteProcess(instanceStr);
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
     * 检查是否有未下载完的数据并设置下次下载的开始时间
     */
    private void umCompleteProcess(String instanceStr) {
        List<Map<String, Object>> unCompleteList = getUnCompleteTrans(instanceStr);
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


}