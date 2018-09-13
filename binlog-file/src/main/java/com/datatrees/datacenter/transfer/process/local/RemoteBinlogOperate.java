package com.datatrees.datacenter.transfer.process.local;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.transfer.bean.LocalBinlogInfo;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.process.TransferTimerTaskCopy;
import com.datatrees.datacenter.transfer.utility.SshUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

public class RemoteBinlogOperate implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(RemoteBinlogOperate.class);
    public static Properties properties = PropertiesUtility.defaultProperties();
    private static final int PORT = Integer.valueOf(properties.getProperty("PORT", "22"));
    private static final String DATABASE = properties.getProperty("jdbc.database", "binlog");
    private static final String SERVER_BASEDIR = properties.getProperty("SERVER_ROOT", "/data1/application/binlog-process/log");
    private static final String CLIENT_BASEDIR = properties.getProperty("CLIENT_ROOT", "/Users/personalc/test/");
    private static final String HDFS_PATH = properties.getProperty("HDFS_ROOT");
    private String hostIp;
    private boolean recordExist = false;


    public static Map<String, String> getHostFileMap() {
        Map<String, String> hostFileMap = new HashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select ");
        stringBuilder.append(LocalBinlogInfo.dbInstance);
        stringBuilder.append(",");
        stringBuilder.append(LocalBinlogInfo.fileName);
        stringBuilder.append(" from ");
        stringBuilder.append(LocalBinlogInfo.lastDownloadFileTable);
        List<Map<String, Object>> dataRecord = null;
        try {
            dataRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), DATABASE, stringBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (dataRecord != null) {
            dataRecord.stream().forEach(x -> hostFileMap.put(String.valueOf(x.get(LocalBinlogInfo.dbInstance)), String.valueOf(x.get(LocalBinlogInfo.fileName))));
        }
        return hostFileMap;
    }


    @Override
    public void run() {
        try {
            TransferTimerTaskCopy.processingMap.put(hostIp, 1);
            Connection connection = new Connection(hostIp, PORT);
            List<String> fileList = SshUtil.getFileList(SERVER_BASEDIR, connection);
            if (null != fileList && fileList.size() > 1) {
                List<String> subFileList = null;
                long batchStart = System.currentTimeMillis();
                Map<String, String> hostFileMap = getHostFileMap();
                if (null != hostFileMap && hostFileMap.size() > 0) {
                    String lastFileName = hostFileMap.get(hostIp);
                    if (lastFileName != null) {
                        LOG.info("The last download binlog file of :" + hostIp + " is :" + lastFileName);
                        int lastIndex = fileList.indexOf(lastFileName);
                        if (fileList.size() - 1 > lastIndex) {
                            subFileList = fileList.subList(lastIndex + 1, fileList.size() - 1);
                            recordExist = true;
                        }
                        if (lastIndex == -1) {
                            if (fileList.size() > 1) {
                                subFileList = fileList.subList(0, fileList.size() - 1);
                            }
                        }
                    } else {
                        LOG.info("No binlog download record find in the database");
                        if (fileList.size() > 1) {
                            subFileList = fileList.subList(0, fileList.size() - 1);
                        }
                    }
                } else {
                    if (fileList.size() > 1) {
                        subFileList = fileList.subList(0, fileList.size() - 1);
                    }
                }
                if (null != subFileList && subFileList.size() > 0) {
                    LOG.info("The binlog files need to download : " + subFileList.toString());
                    Map<String, Object> valueMap = new HashMap<>(6);
                    valueMap.put(TableInfo.DB_INSTANCE, hostIp);
                    valueMap.put(TableInfo.HOST, IPUtility.ipAddress());
                    valueMap.put(TableInfo.BATCH_ID, TimeUtil.timeStamp2DateStr(batchStart, "yyyy-MM-dd HH:mm:ss"));
                    Map<String, Object> lastValueMap = new HashMap<>(3);
                    lastValueMap.put(LocalBinlogInfo.downloadIp, IPUtility.ipAddress());
                    Map<String, Object> whereMap = new HashMap<>(1);
                    whereMap.put(LocalBinlogInfo.dbInstance, hostIp);
                    String hdfsFilePath = HDFS_PATH + hostIp;

                    for (int i = 0; i < subFileList.size(); i++) {
                        String fileName = subFileList.get(i);
                        String remoteFilePath = SERVER_BASEDIR + fileName;
                        long downStart = System.currentTimeMillis();
                        LOG.info("Start download file: " + fileName);
                        SshUtil.getFile(remoteFilePath, CLIENT_BASEDIR + hostIp, connection);
                        long downEnd = System.currentTimeMillis();
                        valueMap.put(TableInfo.DOWN_START_TIME, TimeUtil.stampToDate(downStart));
                        valueMap.put(TableInfo.DOWN_END_TIME, TimeUtil.stampToDate(downEnd));
                        String localFilePath = CLIENT_BASEDIR + hostIp + File.separator + fileName;
                        File localFile = new File(localFilePath);
                        if (localFile.isFile() && localFile.exists()) {
                            Boolean uploadFlag = HDFSFileUtility.put2HDFS(localFilePath, hdfsFilePath, HDFSFileUtility.conf);
                            if (uploadFlag) {
                                LOG.info("File ：" + fileName + " upload to HDFS successful！");
                                valueMap.put(TableInfo.FILE_NAME, fileName);
                                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), DATABASE, LocalBinlogInfo.binlogDownloadRecordTable, valueMap);

                                lastValueMap.put(LocalBinlogInfo.fileName, fileName);
                                long uploadTime = System.currentTimeMillis();
                                lastValueMap.put(LocalBinlogInfo.downloadTime, TimeUtil.stampToDate(uploadTime));
                                if (!recordExist && i == 0) {
                                    lastValueMap.put(LocalBinlogInfo.dbInstance, hostIp);
                                    DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), DATABASE, LocalBinlogInfo.lastDownloadFileTable, lastValueMap);
                                } else {
                                    DBUtil.update(DBServer.DBServerType.MYSQL.toString(), DATABASE, LocalBinlogInfo.lastDownloadFileTable, lastValueMap, whereMap);
                                }
                                // TODO: 2018/9/10 发送至消息队列
                                /*String filePath = hdfsFilePath + File.separator + fileName;
                                TaskDispensor.defaultDispensor().dispense(new Binlog(filePath, hostIp + "_" + fileName, ""));*/
                            } else {
                                LOG.info("File ：" + fileName + "upload to HDFS failed！");
                            }
                        } else {
                            LOG.info("File:" + localFile + " does not exist!");
                        }
                    }
                }
            } else {
                LOG.info("No binlog find in the host : " + hostIp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            TransferTimerTaskCopy.processingMap.put(hostIp, 0);
        }
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }
}

