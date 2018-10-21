package com.datatrees.datacenter.transfer.utility;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.jcraft.jsch.*;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class SFTPUtil {
    private static Session session = null;
    private static ChannelSftp channel = null;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(SFTPUtil.class);
    private static Properties properties = PropertiesUtility.defaultProperties();


    public static ChannelSftp getConnect(String hostIp) {
        String ftpHost = hostIp;
        String port = properties.getProperty("PORT");
        String ftpUserName = properties.getProperty("USER_NAME");
        String ftpPassword = properties.getProperty("PASSWORD");
        int ftpPort = 22;
        if (port != null && !port.equals("")) {
            ftpPort = Integer.valueOf(port);
        }
        JSch jsch = new JSch();
        try {
            logger.info("sftp [ ftpHost = " + ftpHost + "  ftpPort = " + ftpPort + "  ftpUserName = " + ftpUserName + "  ftpPassword = " + ftpPassword + " ]");
            session = jsch.getSession(ftpUserName, ftpHost, ftpPort);
            logger.info("Session created.");
            if (ftpPassword != null) {
                session.setPassword(ftpPassword);
            }
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            logger.info("Session connected.");
            logger.info("Opening SFTP Channel.");
            // 打开SFTP通道
            channel = (ChannelSftp) session.openChannel("sftp");
            // 建树SFTP通道的连接
            channel.connect();
            logger.debug("Connected successfully to ftpHost = " + ftpHost + ",as ftpUserName = "
                    + ftpUserName + ", returning: " + channel);
        } catch (JSchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error("sftp getConnect error : " + e);
        }
        return channel;
    }

    public static void closeChannel() throws Exception {
        try {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        } catch (Exception e) {
            logger.error("close sftp error", e);
            throw new Exception("close ftp error.");
        }
    }


    public static void uploadFile(String localFile, String newName, String remoteFoldPath) throws Exception {
        InputStream input = null;
        try {
            input = new FileInputStream(new File(localFile));
            // 改变当前路径到指定路径
            channel.cd(remoteFoldPath);
            channel.put(input, newName);
        } catch (Exception e) {
            logger.error("Upload file error", e);
            throw new Exception("Upload file error.");
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    throw new Exception("Close stream error.");
                }
            }
        }
    }


    public static void downloadFile(String remoteFile, String remotePath, String localFile) {
        logger.info("sftp download File remotePath :" + remotePath + File.separator + remoteFile + " to localPath : " + localFile + " !");
        OutputStream output = null;
        try {
            File file = new File(localFile);
            File fileParent = file.getParentFile();
            if(!fileParent.exists()) {
                fileParent.mkdirs();
            }
            file.createNewFile();
            output = new FileOutputStream(file);
            channel.cd(remotePath);
            channel.get(remoteFile, output);
        } catch (Exception e) {
            logger.error("Download file error", e);
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    logger.error("Close stream error.");
                }
            }

        }
    }


    @SuppressWarnings("unchecked")
    public static Vector<ChannelSftp.LsEntry> listFiles(String remotePath) throws Exception {
        Vector<ChannelSftp.LsEntry> directoryEntries;
        try {
            directoryEntries = channel.ls(remotePath);
        } catch (SftpException e) {
            logger.error("List file error", e);
            throw new Exception("list file error.");
        }
        return directoryEntries;
    }


    private static boolean checkFileExist(String localPath) {
        File file = new File(localPath);
        return file.exists();
    }

   /* public static void main(String[] args) throws Exception {
        SFTPUtil ftpUtil = new SFTPUtil();
        ChannelSftp channeltest = getConnect();
        System.out.println(channeltest.isConnected());
        System.out.println(System.currentTimeMillis());
        Vector<ChannelSftp.LsEntry> directoryEntries = listFiles("/app/mysql/log/binlog/");
        if (null != directoryEntries && directoryEntries.size() > 0) {

            for (ChannelSftp.LsEntry lsEntry : directoryEntries) {
                if (!lsEntry.getAttrs().isDir()) {
                    System.out.println(lsEntry.getAttrs().isFifo());
                    System.out.println(lsEntry.getLongname());
                    Log.info("download :" + lsEntry.getFilename());
                    downloadFile(lsEntry.getFilename(), "/app/mysql/log/binlog/", "/Users/personalc/test/bin-log.004455");
                    System.out.println(System.currentTimeMillis());
                }
            }
        }
        closeChannel();
        System.out.println(channeltest.isConnected());
    }*/
}
