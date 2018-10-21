package com.datatrees.datacenter.transfer.process;

import ch.ethz.ssh2.Connection;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.LocalBinlogInfo;
import com.datatrees.datacenter.transfer.bean.LocalCenterInfo;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.utility.SFTPUtil;
import com.datatrees.datacenter.transfer.utility.SshUtil;
import com.jcraft.jsch.ChannelSftp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;


/**
 * @author personalc
 */
public class RemoteBinlogOperate implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(RemoteBinlogOperate.class);
    private String hostIp;
    private boolean recordExist = false;


    private static Map<String, String> getHostFileMap() {
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
            dataRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), LocalCenterInfo.DATABASE, stringBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (dataRecord != null) {
            dataRecord.forEach(x -> hostFileMap.put(String.valueOf(x.get(LocalBinlogInfo.dbInstance)), String.valueOf(x.get(LocalBinlogInfo.fileName))));
        }
        return hostFileMap;
    }


    @Override
    public void run() {
        try {
            TransferTimerTask.processingMap.put(hostIp, 1);
            Connection connection = new Connection(hostIp, LocalCenterInfo.PORT);
           /* ChannelSftp channelSftp = SFTPUtil.getConnect(hostIp);
            LOG.info("SFTP is connect:" + channelSftp.isConnected());
            Vector<ChannelSftp.LsEntry> lsEntryVector = SFTPUtil.listFiles(LocalCenterInfo.SERVER_BASEDIR);
            List<String> fileList = new ArrayList<>();
            lsEntryVector.parallelStream().filter(x -> !x.getAttrs().isDir()).forEachOrdered(x -> fileList.add(x.getFilename()));*/
            List<String> fileList = SshUtil.getFileList(LocalCenterInfo.SERVER_BASEDIR, connection);
            if (null != fileList && fileList.size() > 1) {
                List<String> subFileList = null;
                long batchStart = System.currentTimeMillis();
                Map<String, String> hostFileMap = getHostFileMap();
                if (null != hostFileMap && hostFileMap.size() > 0) {
                    String lastFileName = hostFileMap.get(hostIp);
                    if (lastFileName != null) {
                        LOG.info("The last download binlog file of :" + hostIp + " is :" + lastFileName);
                        int lastIndex = fileList.indexOf(lastFileName.substring(lastFileName.indexOf("-") + 1, lastFileName.length()));
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
                    String hdfsFilePath = LocalCenterInfo.HDFS_PATH + File.separator + hostIp;

                    Map<String, Object> processRecordMap = new HashMap<>(3);
                    processRecordMap.put(TableInfo.DB_INSTANCE, hostIp);

                    for (int i = 0; i < subFileList.size(); i++) {
                        String fileName = subFileList.get(i);
                        String fileNameWithTime = System.currentTimeMillis() + "-" + subFileList.get(i);
                        String filePath = hdfsFilePath + File.separator + fileNameWithTime;
                        processRecordMap.put(TableInfo.FILE_NAME, fileNameWithTime);
                        String remoteFilePath = LocalCenterInfo.SERVER_BASEDIR + File.separator + fileName;
                        long requestStart = System.currentTimeMillis();
                        LOG.info("Start download file: " + fileNameWithTime);
                        // TODO: 2018/10/19
                        //SFTPUtil.downloadFile(fileName, LocalCenterInfo.SERVER_BASEDIR, LocalCenterInfo.CLIENT_BASEDIR + File.separator + fileName);
                        SshUtil.getFile(remoteFilePath, LocalCenterInfo.CLIENT_BASEDIR + File.separator + hostIp, connection);
                        File oldFile = new File(LocalCenterInfo.CLIENT_BASEDIR + File.separator + hostIp + File.separator + fileName);
                        File fileNew = new File(LocalCenterInfo.CLIENT_BASEDIR + File.separator + hostIp + File.separator + fileNameWithTime);
                        oldFile.renameTo(fileNew);
                        valueMap.put(TableInfo.REQUEST_START, TimeUtil.stampToDate(requestStart));
                        String localFilePath = LocalCenterInfo.CLIENT_BASEDIR + File.separator + hostIp + File.separator + fileNameWithTime;
                        File localFile = new File(localFilePath);
                        if (localFile.isFile() && localFile.exists()) {
                            Boolean uploadFlag = HDFSFileUtility.put2HDFS(localFilePath, hdfsFilePath, HDFSFileUtility.conf);
                            if (uploadFlag) {
                                LOG.info("File ：" + fileNameWithTime + " upload to HDFS successful！");
                                long requestEnd = System.currentTimeMillis();
                                valueMap.put(TableInfo.REQUEST_END, TimeUtil.stampToDate(requestEnd));
                                valueMap.put(TableInfo.FILE_NAME, fileNameWithTime);
                                valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
                                long fileSize = HDFSFileUtility.getFileSize(filePath);
                                valueMap.put(TableInfo.FILE_SIZE, fileSize);
                                valueMap.put(TableInfo.DOWN_SIZE, fileSize);

                                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), LocalCenterInfo.DATABASE, LocalBinlogInfo.binlogDownloadRecordTable, valueMap);

                                processRecordMap.put(TableInfo.PROCESS_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), LocalCenterInfo.DATABASE, TableInfo.BINLOG_PROC_TABLE, processRecordMap);

                                lastValueMap.put(LocalBinlogInfo.fileName, fileNameWithTime);
                                long uploadTime = System.currentTimeMillis();
                                lastValueMap.put(LocalBinlogInfo.downloadTime, TimeUtil.stampToDate(uploadTime));
                                if (!recordExist && i == 0) {
                                    lastValueMap.put(LocalBinlogInfo.dbInstance, hostIp);
                                    DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), LocalCenterInfo.DATABASE, LocalBinlogInfo.lastDownloadFileTable, lastValueMap);
                                } else {
                                    DBUtil.update(DBServer.DBServerType.MYSQL.toString(), LocalCenterInfo.DATABASE, LocalBinlogInfo.lastDownloadFileTable, lastValueMap, whereMap);
                                }

                                TaskDispensor.defaultDispensor().dispense(new Binlog(filePath, hostIp + TableInfo.INSTANCE_FILE_SEP + fileNameWithTime, hostIp));
                            } else {
                                LOG.info("File ：" + fileNameWithTime + "upload to HDFS failed！");
                            }
                        } else {
                            LOG.info("File:" + localFile + " does not exist!");
                        }
                    }
                    //SFTPUtil.closeChannel();
                }
            } else {
                LOG.info("No binlog find in the host : " + hostIp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            TransferTimerTask.processingMap.put(hostIp, 0);
        }
    }

    void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }
}

