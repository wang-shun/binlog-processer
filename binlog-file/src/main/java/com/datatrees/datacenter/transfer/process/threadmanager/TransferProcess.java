package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.transfer.bean.HttpAccessStatus;
import com.datatrees.datacenter.transfer.bean.TransInfo;
import com.datatrees.datacenter.transfer.utility.FileUtil;
import com.datatrees.datacenter.transfer.utility.HDFSFileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author personalc
 */
public class TransferProcess {
    private static Logger LOG = LoggerFactory.getLogger(TransferProcess.class);
    private int checkInterval = 3000;
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

    private FileUtil fileUtil = new FileUtil();

    public TransferProcess(TransInfo transInfo) {
        this.transInfo = transInfo;
        String filePath = transInfo.getDestPath() + File.separator + transInfo.getFileName();
        try {
            if (HDFSFileUtil.fileSystem.exists(new Path(filePath))) {
                firstDown = false;
                startPos = HDFSFileUtil.getFileSize(filePath);
            } else {
                startPos = 0;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
        }
        endPos = fileUtil.getFileSize(transInfo.getSrcPath());
    }

    /**
     * 设置传输相关信息，并开启传输线程
     */
    public void startTrans() {
        if (firstDown) {
            //文件长度
            long fileLen = fileUtil.getFileSize(transInfo.getSrcPath());
            if (fileLen == HttpAccessStatus.FILE_SIZE_NOT_KNOWN.getValue()) {
                LOG.info("file size is no known");
                return;
            } else if (fileLen == HttpAccessStatus.FILE_NOT_ACCESSIBLE.getValue()) {
                LOG.info("file can not access");
                return;
            } else {
                startPos = 0;
                endPos = fileLen;
            }
        }
        if (startPos != endPos) {
            TransThread transThread = new TransThread(transInfo.getSrcPath(), transInfo.getDestPath(), startPos, endPos,
                    transInfo.getFileName(), transInfo.getDbInstance().getDBInstanceId());
            LOG.info("start= " + startPos + ",  end= " + endPos);
            ThreadPoolInstance.getExecutors().execute(transThread);
            //停止标志
            boolean stop = false;
            while (!stop) {
                sleep(checkInterval);
                if (!transThread.over) {
                    // 还存在未下载完成的线程
                    break;
                } else {
                    stop = true;
                    LOG.info("file transfer over");
                }
            }
        }
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
            LOG.error(e.getMessage(),e);
        }
    }


}
