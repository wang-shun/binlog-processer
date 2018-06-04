package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.utility.HDFSFileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author personalc
 */
class TransferProcess {
    private static Logger LOG = Logger.getLogger(TransferProcess.class);
    private static final int FILE_SIZE_NOT_KNOWN = -1;
    private static final int FILE_NOT_ACCESSIBLE = -2;
    private static final int HTTP_CONNECTION_RESPONSE_CODE = 400;


    /**
     * 文件信息
     */
    private TransInfo transInfo;
    /**
     * 开始位置
     */
    private long startPos;
    /**
     * 结束位置
     */
    private long endPos;
    /**
     * 是否第一次下载文件
     */
    private boolean firstDown = true;


    TransferProcess(TransInfo transInfo) {
        this.transInfo = transInfo;
        try {
            if (HDFSFileUtil.fileSystem.exists(new Path(transInfo.getDestPath() + File.separator + transInfo.getFileName()))) {
                firstDown = false;
                startPos = HDFSFileUtil.getFileSize(transInfo.getDestPath() + File.separator + transInfo.getFileName());
            } else {
                startPos = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        endPos = getFileSize();
    }

    /**
     * 设置传输相关信息，并开启传输线程
     */
    void startTrans() {
        if (firstDown) {
            //文件长度
            long fileLen = getFileSize();
            if (fileLen == FILE_SIZE_NOT_KNOWN) {
                LOG.info("file size is no known");
                return;
            } else if (fileLen == FILE_NOT_ACCESSIBLE) {
                LOG.info("file can not access");
                return;
            } else {
                startPos = 0;
                endPos = fileLen;
            }
        }

        TransThread transThread = new TransThread(transInfo.getSrcPath(), transInfo.getDestPath(), startPos, endPos,
                transInfo.getFileName(), transInfo.getBinLogFile());
        LOG.info("Thread :" + Thread.currentThread().getName() + ", start= " + startPos + ",  end= " + endPos);
        BinLogTransfer.getExecutors().execute(transThread);
        //停止标志
        boolean stop = false;
        while (!stop) {
            sleep(3000);
            if (!transThread.over) {
                // 还存在未下载完成的线程
                break;
            } else {
                stop = true;

            }
        }
        LOG.info("file transfer over");
    }

    /**
     * 获取文件的大小
     *
     * @return 文件大小
     */
    private long getFileSize() {
        int len = -1;
        try {
            URL url = new URL(transInfo.getSrcPath());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("User-Agent", "custom");

            int respCode = connection.getResponseCode();
            if (respCode >= HTTP_CONNECTION_RESPONSE_CODE) {
                LOG.info("Error Code : " + respCode);
                // 代表文件不可访问
                return FILE_NOT_ACCESSIBLE;
            }

            String header;
            for (int i = 1; ; i++) {
                header = connection.getHeaderFieldKey(i);
                if (header != null) {
                    if ("Content-Length".equals(header)) {
                        len = Integer.parseInt(connection.getHeaderField(header));
                        break;
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }

        LOG.info("the file length:  " + len);
        return len;
    }

    /**
     * 休眠时间
     *
     * @param mills 休眠时间
     */
    private static void sleep(int mills) {
        try {
            Thread.sleep(mills);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
