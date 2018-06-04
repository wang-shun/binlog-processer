package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.datatrees.datacenter.transfer.bean.DownLoadTable;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.utility.DBUtil;
import com.datatrees.datacenter.transfer.utility.FileUtil;
import com.datatrees.datacenter.transfer.utility.HDFSFileUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author personalc
 */
public class TransThread implements Serializable, Runnable {
    private static Logger LOG = Logger.getLogger(TransThread.class);
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



    public TransThread(String src, String dest, long startPos, long endPos, String fileName, DescribeBinlogFilesResponse.BinLogFile binLogFile) {
        this.src = src;
        this.dest = dest;
        this.fileName = fileName;
        this.startPos = startPos;
        this.endPos=endPos;
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
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(DownLoadTable.FILE_NAME, fileName);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(DownLoadTable.DOWN_STATUS, DownloadStatus.COMPLETE.getValue());
        try {
            DBUtil.update(FileUtil.getProperties().getProperty("binlog.record.table"), valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
