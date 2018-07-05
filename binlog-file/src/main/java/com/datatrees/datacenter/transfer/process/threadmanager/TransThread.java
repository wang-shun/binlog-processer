package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.process.TransferTimerTask;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * @author personalc
 */
public class TransThread implements Serializable, Runnable {
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final int BUFFER_SIZE = Integer.parseInt(properties.getProperty("BUFFER_SIZE"));
    private static Logger LOG = LoggerFactory.getLogger(TransThread.class);
    private FileSystem fs = HDFSFileUtility.fileSystem;
    /**
     * 文件所在src
     */
    private String src;
    /**
     * 目标路径
     */
    private String dest;
    /**
     * 文件名
     */
    private String fileName;
    /**
     * 传输开始位置
     */
    private long startPos;
    /**
     * 结束位置
     */
    private long endPos;
    /**
     * 下载完成标志
     */
    boolean over = false;
    /**
     * 实例Id
     */
    private String instanceId;
    /**
     * 下载进度
     */
    private long percent;


    TransThread(String src, String dest, long startPos, long endPos, String fileName, String instanceId) {
        this.src = src;
        this.dest = dest;
        this.fileName = fileName;
        this.startPos = startPos;
        this.endPos = endPos;
        this.instanceId = instanceId;
    }

    @Override
    public void run() {
        LOG.info("the current thread is: " + Thread.currentThread().getName() + " begin to download file :" + fileName);
        while (startPos < endPos) {
            try {
                URL srcUrl = new URL(src);
                HttpURLConnection httpConnection = (HttpURLConnection) srcUrl.openConnection();
                //下载starPos以后的数据
                String prop = "bytes=" + startPos + "-";
                //设置请求首部字段 RANGE
                httpConnection.setRequestProperty("RANGE", prop);
                LOG.info("start download file : " + fileName + " from :" + prop);
                InputStream input = new BufferedInputStream(httpConnection.getInputStream(), 1000 * BUFFER_SIZE);
                byte[] b = new byte[500 * BUFFER_SIZE];
                int bytes;
                int tries = 20;
                Path dstPath = new Path(dest + File.separator + fileName);
                if (!fs.exists(dstPath)) {
                    fs.create(dstPath).close();
                }
                while ((((bytes = input.read(b))) != -1) && (startPos < endPos)) {
                    FSDataOutputStream out = getFsDataOutputStream(tries, dstPath);
                    int minByte = (int) Math.min(bytes, (endPos - startPos));
                    out.write(b, 0, minByte);
                    out.flush();
                    startPos += minByte;
                    percent = (100 * startPos) / endPos;
                    out.close();
                }
                statusInfo();
                input.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        changeDownStatus();
        insertProcess();
        TransferTimerTask.processingSet.remove(fileName);
        LOG.info("******************************");
        LOG.info("the set size after delete:" + TransferTimerTask.processingSet.size());
        LOG.info("******************************");
        over = true;
    }

    private FSDataOutputStream getFsDataOutputStream(int tries, Path dstPath) {
        boolean recovered = false;
        FSDataOutputStream out = null;
        while (!recovered && tries > 0) {
            try {
                out = fs.append(dstPath);
                recovered = true;
            } catch (IOException e) {
                if (e.getClass().getName().equals(RecoveryInProgressException.class.getName())) {
                    try {
                        LOG.info("sleep 1000 millis and retry to append data to HDFS");
                        Thread.sleep(3000);
                        tries--;
                    } catch (InterruptedException e1) {
                        LOG.error(e1.getMessage(), e1);
                    }
                }
            }

        }
        return out;
    }


    /**
     * 进度条
     */
    private void statusInfo() {
        long num = 0;
        if (percent > num) {
            LOG.info(fileName + "当前下载进度为：" + percent + "%");
        }
        if (percent > 100) {
            //delErrorFile();
            System.gc();
        }
    }

    private void delErrorFile() {
        HDFSFileUtility.checkAndDel(dest + File.separator + fileName);
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(TableInfo.FILE_NAME, fileName);
        whereMap.put(TableInfo.DB_INSTANCE, instanceId);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.UNCOMPLETED.getValue());
        try {
            DBUtil.update(TableInfo.BINLOG_TRANS_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            LOG.error("reset down_status of file:" + fileName + " to 0 failed");
        }
        startPos = 0;
        Thread.currentThread().interrupt();
    }

    /**
     * 下载完毕，修改状态
     */
    private void changeDownStatus() {
        Map<String, Object> whereMap = new HashMap<>(1);
        whereMap.put(TableInfo.FILE_NAME, fileName);
        Map<String, Object> valueMap = new HashMap<>(3);
        valueMap.put(TableInfo.REQUEST_END, TimeUtil.stampToDate(System.currentTimeMillis()));
        valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
        valueMap.put(TableInfo.DOWN_SIZE, HDFSFileUtility.getFileSize(dest + File.separator + fileName));
        try {
            DBUtil.update(TableInfo.BINLOG_TRANS_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将下载完成的binlog文件记录写入t_binlog_process
     */
    private void insertProcess() {
        Map<String, Object> whereMap;
        whereMap = new HashMap<>(2);
        whereMap.put(TableInfo.FILE_NAME, fileName);
        whereMap.put(TableInfo.DB_INSTANCE, instanceId);
        // send to queue
        try {
            String path = dest + File.separator + fileName;
            TaskDispensor.defaultDispensor().dispense(new Binlog(path, instanceId + "_" + fileName, DBInstanceUtil.getConnectString(instanceId)));
        } catch (Exception e) {
            LOG.error("send " + fileName + " to queue failed");
        }
        try {
            List<Map<String, Object>> processRecord = DBUtil.query(TableInfo.BINLOG_PROC_TABLE, whereMap);
            if (processRecord.size() == 0) {
                Map<String, Object> map = new HashMap<>(5);
                map.put(TableInfo.FILE_NAME, fileName);
                map.put(TableInfo.DB_INSTANCE, instanceId);
                map.put(TableInfo.BAK_INSTANCE_ID, DBInstanceUtil.getBackInstanceId(instanceId));
                map.put(TableInfo.PROCESS_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                try {
                    DBUtil.insert(TableInfo.BINLOG_PROC_TABLE, map);
                } catch (SQLException e) {
                    LOG.error("insert " + fileName + "to t_binlog_process failed");
                }
            }
        } catch (Exception e) {
            LOG.error("query from database error");
        }
    }

}
