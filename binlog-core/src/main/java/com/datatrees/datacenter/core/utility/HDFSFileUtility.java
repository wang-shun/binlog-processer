package com.datatrees.datacenter.core.utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author personalc
 */
public class HDFSFileUtility {
    private static Logger LOG = LoggerFactory.getLogger(HDFSFileUtility.class);
    public static Configuration conf = null;

    static {
        if (null == conf) {
            conf = new Configuration();
            conf.setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enabled", true);
            conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                    1000);
            conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
            conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
        }
    }

    /**
     * 文件上传
     *
     * @param src  本地路径
     * @param des  HDFS路径
     * @param conf HDFS配置
     * @return 上传是否成功
     */
    public static boolean put2HDFS(String src, String des, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(des), conf);
            Path desPath = new Path(des);
            fileSystem.copyFromLocalFile(false, new Path(src), desPath);

        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 文件下载
     *
     * @param src  本地路径
     * @param dst  HDFS路径
     * @param conf HDFS配置
     * @return 下载是否成功
     */
    public static boolean getFromHDFS(String src, String dst, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(dst), conf);
            Path dstPath = new Path(dst);
            fileSystem.copyToLocalFile(false, new Path(src), dstPath);
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 文件检测并删除
     *
     * @param path HDFS文件路径
     * @return 检测及删除结果
     */
    public static boolean checkAndDel(String path) {

        try {
            FileSystem fileSystem = FileSystem.get(URI.create(path), conf);
            Path dstPath = new Path(path);
            if (fileSystem.exists(dstPath)) {
                fileSystem.delete(dstPath, true);
            } else {
                LOG.info("file: " + path + " does not exist");
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 获取HDFS文件大小
     *
     * @param path
     * @return filesize
     */
    public static long getFileSize(String path) {
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(path), conf);
            Path dstPath = new Path(path);
            if (fileSystem.exists(dstPath)) {
                ContentSummary contentSummary = fileSystem.getContentSummary(dstPath);
                return contentSummary.getLength();
            } else {
                return 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static List<String> printHdfs(FileStatus file, FileSystem fs, List<String> fileList) {
        //如果为文件夹，则打印其hdfs路
        if (file.isDirectory()) {
            System.out.println(file.getPath());
            //得到该路径下的文件
            try {
                FileStatus[] files = fs.listStatus(file.getPath());
                //如果该路径下仍然有文件，则递归调用打印函数
                if (files.length > 0) {
                    for (FileStatus f : files) {
                        printHdfs(f, fs, fileList);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            fileList.add(file.getPath().toString());
            System.out.println(fileList.size());
            System.out.println(file.getPath().toUri());
        }
        return fileList;
    }

    public static List<String> getFilesPath(String path) {
        List<String> fileList = new ArrayList<>();
        //从hdfs根路径开始
        try {
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            FileStatus[] files = fs.listStatus(new Path(path));
            //开始调用打印函数
            for (FileStatus file : files) {
                String filePath=file.getPath().toString();
                if(fs.isFile(file.getPath())){
                    fileList.add(filePath);
                }
                else {
                    fileList.addAll(printHdfs(file, fs, fileList));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileList;
    }
    public static FileSystem getFileSystem(String path){
        FileSystem fs = null;
        try {
           fs = FileSystem.get(URI.create(path),conf);
        } catch (IOException e) {
            Log.info(e.getMessage());
        }
        return fs;
    }
}
