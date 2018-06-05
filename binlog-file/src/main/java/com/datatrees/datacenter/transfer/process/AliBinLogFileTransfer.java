package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.utility.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 根据条件下载指定实例binlog文件
 *
 * @author personalc
 */
public class AliBinLogFileTransfer implements TaskRunner, BinlogFileTransfer {
    private static Logger LOG = LoggerFactory.getLogger(AliBinLogFileTransfer.class);
    private static Properties properties = FileUtil.getProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private static final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private static final String HDFS_PATH = properties.getProperty("HDFS_PATH");
    private static final long DOWN_TIME_INTER = Long.valueOf(properties.getProperty("down.time.inter.secends"));
    private static final String BINLOG_TRANS_TABLE = TableInfo.BINLOG_TRANS_TABLE;
    private static final String BINLOG_PROC_TABLE = TableInfo.BINLOG_PROC_TABLE;
    private static final DefaultProfile profile;
    private static final IAcsClient client;
    private static String startTime;
    private static String endTime;

    static {
        profile = DefaultProfile.getProfile(
                REGION_ID,
                ACCESS_KEY_ID,
                ACCESS_SECRET);
        client = new DefaultAcsClient(profile);
    }

    public static void main(String[] args) {
        // 创建API请求并设置参数
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setActionName(BINLOG_ACTION_NAME);
        //重新下载未完成的数据
        List<Map<String, Object>> unCompleteList = getUnCompleteTrans();
        if (unCompleteList.size() > 0 && unCompleteList != null) {
            processUnComplete(distinctUnCompleteTrans(unCompleteList));
        }
        //开始正常下载
        long currentTime = System.currentTimeMillis();
        startTime = TimeUtil.timeStamp2DateStr(currentTime - DOWN_TIME_INTER * 6000, TableInfo.UTC_FORMAT);
        binlogFilesRequest.setStartTime(startTime);
        LOG.info(startTime);
        endTime = TimeUtil.timeStamp2DateStr(currentTime, TableInfo.UTC_FORMAT);
        binlogFilesRequest.setEndTime(endTime);
        LOG.info(endTime);
        List<DBInstance> instances = DBInstanceUtil.getAllPrimaryDBInstance();
        for (DBInstance dbInstance : instances) {
            instanceBinlogTrans(profile, client, binlogFilesRequest, dbInstance);
        }
        getExecutors().shutdown();
        try {
            getExecutors().awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载单个实例binlog文件
     *
     * @param profile
     * @param client
     * @param binlogFilesRequest
     * @param dbInstance
     */
    private static void instanceBinlogTrans(DefaultProfile profile, IAcsClient client, DescribeBinlogFilesRequest binlogFilesRequest, DBInstance dbInstance) {

        binlogFilesRequest.setDBInstanceId(dbInstance.getDBInstanceId());
        List<BinLogFile> binLogFiles = BinLogFileUtil.getBinLogFiles(client, binlogFilesRequest, profile);
        String bakInstanceId = DBInstanceUtil.getBackInstanceId(dbInstance);
        List<BinLogFile> fileList = binLogFiles.parallelStream()
                .filter(binLogFile -> binLogFile.getHostInstanceID().equals(bakInstanceId)).collect(Collectors.toList());
        long instanceLogSize = fileList.size();
        List<Integer> fileNumList = BinLogFileUtil.getFileNumberFromUrl(fileList, REGEX_PATTERN);
        fileNumList = fileNumList.stream().sorted().collect(Collectors.toList());

        //判断文件编号是否连续
        if (fileList.size() > 0) {
            int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
            if (instanceLogSize == (maxDiff + 1)) {
                for (int i = 0; i < fileList.size(); i++) {
                    BinLogFile binLogFile = fileList.get(i);
                    System.out.println(binLogFile.getChecksum());
                    LOG.info("file size: " + binLogFile.getFileSize());
                    LOG.info("begin download binlog file :" + "[" + binLogFile.getDownloadLink() + "]");
                    String dbInstanceId = dbInstance.getDBInstanceId();
                    String hostInstanceId = binLogFile.getHostInstanceID();

                    String logEndTime = binLogFile.getLogEndTime();
                    String logStartTime = binLogFile.getLogBeginTime();
                    Long LogEndTimeStamp = TimeUtil.utc2TimeStamp(logEndTime);

                    String fileName = LogEndTimeStamp + "-" + BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN);
                    System.out.println(fileName);
                    Map<String, Object> whereMap = new HashMap<>(4);
                    whereMap.put(TableInfo.DB_INSTANCE, dbInstanceId);
                    whereMap.put(TableInfo.FILE_NAME, fileName);
                    try {
                        int recordCount = DBUtil.query(BINLOG_TRANS_TABLE, whereMap).size();
                        if (recordCount == 0) {
                            Map<String, Object> map = new HashMap<>();
                            map.put(TableInfo.DB_INSTANCE, dbInstanceId);
                            map.put(TableInfo.FILE_NAME, fileName);
                            map.put(TableInfo.BAK_INSTANCE_ID, hostInstanceId);
                            map.put(TableInfo.LOG_START_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logStartTime), TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.LOG_END_TIME, TimeUtil.timeStamp2DateStr(TimeUtil.utc2TimeStamp(logEndTime), TableInfo.COMMON_FORMAT));
                            map.put(TableInfo.DOWN_START_TIME, TimeUtil.utc2Common(startTime));
                            map.put(TableInfo.DOWN_END_TIME, TimeUtil.utc2Common(endTime));
                            DBUtil.insert(BINLOG_TRANS_TABLE, map);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    StringBuilder filePath = new StringBuilder();
                    filePath.append(HDFS_PATH)
                            .append(File.separator)
                            .append(dbInstanceId)
                            .append(File.separator)
                            .append(hostInstanceId);
                    TransInfo transInfo = new TransInfo(binLogFile.getDownloadLink(), filePath.toString(),
                            fileName, dbInstance);
                    TransferProcess transferProcess = new TransferProcess(transInfo);
                    transferProcess.startTrans();
                    LOG.info("download binlog file :" + binLogFile.getDownloadLink() + " successfully");
                    try {
                        TaskDispensor.defaultDispensor().dispense(
                                new Binlog(DBInstanceUtil.getConnectString(dbInstance),
                                        dbInstance.getDBInstanceId() + "-"
                                                + fileName.split(".")[0],
                                        filePath.toString() + fileName));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

            } else {
                LOG.info("the downloaded binlog files is not complete");
            }
        }
    }

    /**
     * 返回为传输未完成的binlog文件信息
     */
    public static List<Map<String, Object>> getUnCompleteTrans() {
        Map<String, Object> whereMap = new HashMap<>(1);
        List<Map<String, Object>> resultList = null;
        whereMap.put(TableInfo.DOWN_STATUS, DownloadStatus.UNCOMPLETED.getValue());
        try {
            resultList = DBUtil.query(BINLOG_TRANS_TABLE, whereMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    /**
     * 过滤掉重复时间段的下载数据
     *
     * @param resultList
     * @return
     */
    public static List<Map<String, Object>> distinctUnCompleteTrans(List<Map<String, Object>> resultList) {
        List<String> instanceTimeList = new ArrayList<>();
        Iterator<Map<String, Object>> itr = resultList.iterator();
        while (itr.hasNext()) {
            Map<String, Object> map = itr.next();
            String instanceTime = map.get(TableInfo.DB_INSTANCE).toString() + map.get(TableInfo.DOWN_START_TIME).toString();
            if (instanceTimeList.contains(instanceTime)) {
                itr.remove();
            } else {
                instanceTimeList.add(instanceTime);
            }
        }
        return resultList;
    }

    /**
     * 返回实例名和下载时间组合后的字符串
     *
     * @param binLogRecord 数据库中查询到的一条binlog下载记录
     * @return
     */
    public static String interInstanceAndStartTime(Map<String, Object> binLogRecord) {
        return new String((String) binLogRecord.get(TableInfo.DB_INSTANCE) + TimeUtil.dateToStr((Timestamp) binLogRecord.get(TableInfo.DOWN_START_TIME), TableInfo.COMMON_FORMAT));
    }

    /**
     * 处理未下载完成的的binlog文件
     */
    public static void processUnComplete(List<Map<String, Object>> downLoadInfo) {
        Iterator itr = downLoadInfo.iterator();
        while (itr.hasNext()) {
            Map<String, Object> info = (Map<String, Object>) itr.next();
            startTime = TimeUtil.dateToStr((Timestamp) info.get(TableInfo.DOWN_START_TIME), TableInfo.UTC_FORMAT);
            endTime = TimeUtil.dateToStr((Timestamp) info.get(TableInfo.DOWN_END_TIME), TableInfo.UTC_FORMAT);
            String instanceId = (String) info.get(TableInfo.DB_INSTANCE);
            DBInstance dbInstance = new DBInstance();
            dbInstance.setDBInstanceId(instanceId);
            DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
            binlogFilesRequest.setStartTime(startTime);
            binlogFilesRequest.setEndTime(endTime);
            instanceBinlogTrans(profile, client, binlogFilesRequest, dbInstance);
        }
    }

    private static class LazyHolder {
        private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 10, 3, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }


    public static final ThreadPoolExecutor getExecutors() {
        return LazyHolder.executor;
    }


    @Override
    public void process() {

    }

    @Override
    public void transfer() {

    }

    void send() {
// TODO: 2018/6/5  build binlog

//        Binlog binlog = new Binlog();
//        TaskProcessor
    }
}