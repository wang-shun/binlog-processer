package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import com.datatrees.datacenter.transfer.utility.HDFSFileUtil;
import com.datatrees.datacenter.transfer.utility.TimeUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.datatrees.datacenter.transfer.bean.TableInfo.BINLOG_PROC_TABLE;
import static com.datatrees.datacenter.transfer.bean.TableInfo.BINLOG_TRANS_TABLE;

/**
 * @author personalc
 */
public class TransThread implements Serializable, Runnable {
    private static Logger LOG = LoggerFactory.getLogger(TransThread.class);
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
        while (startPos < endPos && !over) {
            try {
                URL ourl = new URL(src);
                HttpURLConnection httpConnection = (HttpURLConnection) ourl.openConnection();
                //下载starPos以后的数据
                String prop = "bytes=" + startPos + "-";
                //设置请求首部字段 RANGE
                httpConnection.setRequestProperty("RANGE", prop);
                LOG.info(prop);
                InputStream input = httpConnection.getInputStream();
                byte[] b = new byte[1024];
                int bytes;
                int tries = 60;
                boolean recovered = false;

                Path dstPath = new Path(dest + File.separator + fileName);
                FileSystem fs = HDFSFileUtil.fileSystem;
                if (!fs.exists(dstPath)) {
                    fs.create(dstPath).close();
                }
                while (!recovered && tries > 0) {
                    while ((((bytes = input.read(b))) > 0) && (startPos < endPos) && !over) {
                        try {
                            FSDataOutputStream out = fs.append(dstPath);
                            out.write(b, 0, bytes);
                            startPos += bytes;
                            recovered = true;
                            out.close();
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
                        }
                    }
                }
                LOG.info("Thread " + Thread.currentThread().getName() + " is done");
                over = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // update a the record
        Map<String, Object> whereMap = new HashMap<>();
        whereMap.put(TableInfo.FILE_NAME, fileName);
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
        try {
            DBUtil.update(BINLOG_TRANS_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // insert a record to t_binlog_process
        long currentTime = System.currentTimeMillis();
        String processStart = TimeUtil.timeStamp2DateStr(currentTime, TableInfo.UTC_FORMAT);
        Map<String, Object> map = new HashMap<>(5);
        map.put(TableInfo.FILE_NAME, fileName);
        map.put(TableInfo.BAK_INSTANCE_ID, DBInstanceUtil.getBackInstanceId(instanceId));
        map.put(TableInfo.DOWN_START_TIME, TimeUtil.strToDate(processStart, TableInfo.COMMON_FORMAT));
        try {
            DBUtil.insert(BINLOG_PROC_TABLE, map);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // send to queue
        try {
            TaskDispensor.defaultDispensor().dispense(
                    new Binlog(dest + File.separator + fileName,
                            instanceId + "_"
                                    + fileName,
                            DBInstanceUtil.getConnectString(instanceId)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
