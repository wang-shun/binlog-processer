package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import com.datatrees.datacenter.transfer.utility.HDFSFileUtil;
import com.datatrees.datacenter.transfer.utility.TimeUtil;
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

import static com.datatrees.datacenter.transfer.bean.TableInfo.BINLOG_PROC_TABLE;
import static com.datatrees.datacenter.transfer.bean.TableInfo.BINLOG_TRANS_TABLE;

/**
 * @author personalc
 */
public class TransThread implements Serializable, Runnable {
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final int BUFFER_SIZE = Integer.parseInt(properties.getProperty("BUFFER_SIZE"));
    private static Logger LOG = LoggerFactory.getLogger(TransThread.class);
    FileSystem fs = HDFSFileUtil.fileSystem;
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
     * 分段传输的开始位置
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
     * 实例id
     */
    private String instanceId;


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
        while (startPos < endPos && !over) {
            try {
                URL surl = new URL(src);
                HttpURLConnection httpConnection = (HttpURLConnection) surl.openConnection();
                //下载starPos以后的数据
                String prop = "bytes=" + startPos + "-";
                //设置请求首部字段 RANGE
                httpConnection.setRequestProperty("RANGE", prop);
                LOG.info(prop);
                InputStream input = new BufferedInputStream(httpConnection.getInputStream(), 1000 * BUFFER_SIZE);
                byte[] b = new byte[500 * BUFFER_SIZE];
                int bytes;
                int tries = 5;
                boolean recovered = false;
                Path dstPath = new Path(dest + File.separator + fileName);
                if (!fs.exists(dstPath)) {
                    fs.create(dstPath).close();
                }
                FSDataOutputStream out = null;
                while (!recovered && tries > 0) {
                    while ((((bytes = input.read(b))) != -1) && (startPos < endPos)) {
                        try {
                            out = fs.append(dstPath);
                            int minByte = (int) Math.min(bytes, (endPos - startPos));
                            out.write(b, 0, minByte);
                            startPos += minByte;
                            recovered = true;
                        } catch (IOException e) {
                            if (e.getClass().getName().equals(RecoveryInProgressException.class.getName())) {
                                try {
                                    LOG.info("sleep 1000 millis and retry to append data to HDFS");
                                    Thread.sleep(1000);
                                    tries--;
                                } catch (InterruptedException e1) {
                                    e1.printStackTrace();
                                }
                            }
                        } finally {
                            if (out != null) {
                                out.close();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        over = true;
        LOG.info("Thread " + Thread.currentThread().getName() + " is done");
        // update a the record
        if (startPos == endPos && over) {
            Map<String, Object> whereMap = new HashMap<>(1);
            whereMap.put(TableInfo.FILE_NAME, fileName);
            Map<String, Object> valueMap = new HashMap<>(1);
            valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
            try {
                DBUtil.update(BINLOG_TRANS_TABLE, valueMap, whereMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // insert a record to t_binlog_process
            whereMap = new HashMap<>(2);
            whereMap.put(TableInfo.FILE_NAME, fileName);
            whereMap.put(TableInfo.DB_INSTANCE, instanceId);
            try {
                List<Map<String, Object>> processRecord = DBUtil.query(TableInfo.BINLOG_PROC_TABLE, whereMap);
                if (processRecord.size() == 0) {
                    long currentTime = System.currentTimeMillis();
                    String processStart = TimeUtil.timeStamp2DateStr(currentTime, TableInfo.UTC_FORMAT);
                    Map<String, Object> map = new HashMap<>(5);
                    map.put(TableInfo.FILE_NAME, fileName);
                    map.put(TableInfo.DB_INSTANCE, instanceId);
                    map.put(TableInfo.BAK_INSTANCE_ID, DBInstanceUtil.getBackInstanceId(instanceId));
                    map.put(TableInfo.PROCESS_START, TimeUtil.strToDate(processStart, TableInfo.UTC_FORMAT));
                    try {
                        DBUtil.insert(BINLOG_PROC_TABLE, map);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    // send to queue
                    try {
                        String path = dest + File.separator + fileName;
                        TaskDispensor.defaultDispensor().dispense(new Binlog(path, instanceId + "_" + fileName, DBInstanceUtil.getConnectString(instanceId)));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
