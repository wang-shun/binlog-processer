package com.datatrees.datacenter.transfer.process;

import ch.ethz.ssh2.Connection;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.LocalCenterInfo;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.core.threadpool.ThreadPoolInstance;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import com.datatrees.datacenter.transfer.utility.SshUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class LocalDataCenterTransfer extends BinlogFileTransfer {
    private static Logger LOG = LoggerFactory.getLogger(LocalDataCenterTransfer.class);

    @Override
    public void process() {

    }

    @Override
    public void transfer() {
        ExecutorService executorService = ThreadPoolInstance.getExecutors();
        checkDataConsistency();
        //处理解析错误文件
        String resolveError = "select * from " + TableInfo.BINLOG_PROC_TABLE + " where status<>1 and status<>0 and status<>7 and retry_times=" + retryTimes;
        processErrorFile(resolveError);
        List<String> ipList = Arrays.asList(LocalCenterInfo.Ips);
        String ipStr = DBInstanceUtil.getInstancesString(ipList);
        List<Map<String, Object>> unCompleteList = this.getUnCompleteTrans(ipStr);
        if (null != unCompleteList && unCompleteList.size() > 0) {
            for (Map<String, Object> record : unCompleteList) {
                processRetry(record);
            }
        }
        for (String ip : LocalCenterInfo.Ips) {
            LOG.info("start download binlog from :" + ip);
            System.out.println(TransferTimerTask.processingMap.toString());
            if ((null == TransferTimerTask.processingMap.get(ip)) || (TransferTimerTask.processingMap.get(ip) == 0)) {
                RemoteBinlogOperate remoteBinlogOperate = new RemoteBinlogOperate();
                remoteBinlogOperate.setHostIp(ip.trim());
                executorService.execute(remoteBinlogOperate);
            }
        }
    }

    private void processRetry(Map<String, Object> record) {
        Connection connection;
        String hostIP = String.valueOf(record.get(TableInfo.DB_INSTANCE));
        String fileName = String.valueOf(record.get(TableInfo.FILE_NAME));
        String fileWithoutTime = fileName.substring(fileName.indexOf("-") + 1, fileName.length());
        connection = new Connection(hostIP, LocalCenterInfo.PORT);
        SshUtil.getFile(LocalCenterInfo.SERVER_BASEDIR + File.separator + fileWithoutTime, LocalCenterInfo.CLIENT_BASEDIR, connection);
        //文件重命名
        File fileOld = new File(LocalCenterInfo.CLIENT_BASEDIR + File.separator + fileWithoutTime);
        String localFilePath = LocalCenterInfo.CLIENT_BASEDIR + File.separator + hostIP + File.separator + fileName;
        File localFile = new File(localFilePath);
        fileOld.renameTo(localFile);
        String hdfsFilePath = LocalCenterInfo.HDFS_PATH + File.separator + hostIP;
        if (localFile.isFile() && localFile.exists()) {
            Boolean uploadFlag = HDFSFileUtility.put2HDFS(localFilePath, hdfsFilePath, HDFSFileUtility.conf);
            if (uploadFlag) {
                Map<String, Object> whereMap = new HashMap<>(2);
                whereMap.put(TableInfo.FILE_NAME, fileName);
                whereMap.put(TableInfo.DB_INSTANCE, hostIP);
                Map<String, Object> valueMap = new HashMap<>(3);
                valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
                valueMap.put(TableInfo.DOWN_SIZE, HDFSFileUtility.getFileSize(hdfsFilePath + File.separator + fileName));
                valueMap.put(TableInfo.REQUEST_END, TimeUtil.stampToDate(System.currentTimeMillis()));
                try {
                    DBUtil.update(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_TRANS_TABLE, valueMap, whereMap);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                // send to queue
                try {
                    String path = hdfsFilePath + File.separator + fileName;
                    TaskDispensor.defaultDispensor().dispense(new Binlog(path, hostIP + TableInfo.INSTANCE_FILE_SEP + fileName, hostIP));
                } catch (Exception e) {
                    LOG.error("send " + fileName + " to queue failed");
                }
                //update database
                try {
                    List<Map<String, Object>> processRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, whereMap);
                    if (processRecord.size() == 0) {
                        Map<String, Object> map = new HashMap<>(5);
                        map.put(TableInfo.FILE_NAME, fileName);
                        map.put(TableInfo.DB_INSTANCE, hostIP);
                        map.put(TableInfo.PROCESS_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                        try {
                            DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, map);
                        } catch (SQLException e) {
                            LOG.error("insert " + fileName + "to t_binlog_process failed");
                        }
                    } else {
                        LOG.info("File :" + fileName + " is processing");
                    }
                } catch (Exception e) {
                    LOG.error("query from database error");
                }

            } else {
                LOG.info("File: " + fileName + " upload to HDFS failed!");
            }
        }
    }

    /**
     * 返回传输未完成的binlog文件信息
     */
    @Override
    public List<Map<String, Object>> getUnCompleteTrans(String instanceStr) {
        List<Map<String, Object>> resultList = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("select *")
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
                    .append(")");
            resultList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return resultList;
    }

}
