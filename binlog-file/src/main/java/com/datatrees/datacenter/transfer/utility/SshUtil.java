package com.datatrees.datacenter.transfer.utility;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SshUtil {

    private static Logger LOG = LoggerFactory.getLogger(SshUtil.class);
    public static Properties properties = PropertiesUtility.defaultProperties();
    private static final String USER = properties.getProperty("USER_NAME", "root");
    private static final String PASSWORD = properties.getProperty("PASSWORD", "");

    /**
     * 本机的私钥文件
     */
    private static String PRIVATEKEY = properties.getProperty("PRIVATE_KEY", "/Users/personalc/.ssh/id_rsa");
    /**
     * 使用用户名和密码来进行登录验证。如果为true则通过用户名和密码登录，false则使用rsa免密码登录
     */
    private static boolean usePassword = Boolean.parseBoolean(properties.getProperty("USE_PASSWORD", "true"));

    public static boolean isAuthedWithPassword(String user, String password, Connection connection) {
        try {
            return connection.authenticateWithPassword(user, password);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false;
    }


    public static boolean isAuthedWithPublicKey(String user, File privateKey, String password, Connection connection) {
        try {
            return connection.authenticateWithPublicKey(user, privateKey, password);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false;
    }

    private static boolean isAuth(Connection connection) {
        if (usePassword) {
            return isAuthedWithPassword(USER, PASSWORD, connection);
        } else {
            return isAuthedWithPublicKey(USER, new File(PRIVATEKEY), PASSWORD, connection);
        }
    }

    public static void getFile(String remoteFile, String localDirectory, Connection connection) {
        try {
            connection.connect();
            boolean isAuthed = isAuth(connection);
            if (isAuthed) {
                LOG.info(connection.getHostname() + " 认证成功!");
                SCPClient scpClient = connection.createSCPClient();
                File localPath = new File(localDirectory);
                if (!localPath.exists() && !localPath.isDirectory()) {
                    LOG.info("文件夹：" + localDirectory + " 不存在！");
                    LOG.info("创建文件夹：" + localDirectory + " ...");
                    boolean flag = localPath.mkdirs();
                    if (flag) {
                        LOG.info("文件夹：" + localDirectory + " 创建成功");
                    } else {
                        LOG.error("文件夹：" + localDirectory + " 创建失败");
                    }
                }
                scpClient.get(remoteFile, localDirectory);
                LOG.info("文件:" + remoteFile + " 下载完毕");
            } else {
                LOG.info(connection.getHostname() + " 认证失败!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    private static void getFiles(String[] remoteFiles, String localDirectory, Connection connection) {
        try {
            connection.connect();
            boolean isAuthed = isAuth(connection);
            if (isAuthed) {
                LOG.info(connection.getHostname() + " 认证成功!");
                SCPClient scpClient = connection.createSCPClient();
                File targetPath = new File(localDirectory);
                if (!targetPath.exists() && !targetPath.isDirectory()) {
                    LOG.info("目标文件夹：" + localDirectory + " 不存在！");
                    LOG.info("创建目标文件夹：" + localDirectory + " ...");
                    boolean flag = targetPath.mkdir();
                    if (flag) {
                        LOG.info("文件夹：" + localDirectory + " 创建成功");
                    } else {
                        LOG.error("文件夹：" + localDirectory + " 创建失败");
                    }
                }
                scpClient.get(remoteFiles, localDirectory);
                LOG.info("从主机: [" + connection.getHostname() + "] 下载文件:" + Arrays.toString(remoteFiles) + " 下载完毕");
            } else {
                LOG.info(connection.getHostname() + " 认证失败!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    public static void putFile(String localFile, String remoteTargetDirectory, Connection connection) {
        try {
            connection.connect();
            boolean isAuthed = isAuth(connection);
            if (isAuthed) {
                SCPClient scpClient = connection.createSCPClient();
                scpClient.put(localFile, remoteTargetDirectory, "0644");
            } else {
                LOG.info(connection.getHostname() + " 认证失败!");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            connection.close();
        }
    }

    public static List<String> getFileList(String filePath, Connection connection) {
        List<String> fileList = null;
        try {
            fileList = new ArrayList<>();
            connection.connect();
            boolean isAuthed = isAuth(connection);
            if (isAuthed) {
                Session sess = connection.openSession();
                //执行 linux 命令,打印出目录下文件名，按修改时间升序排列
                sess.execCommand("cd " + filePath + ";ls -lrt --time-style='+%Y-%m-%d %H:%M:%S' | awk '{print $8}'");
                //获取命令行输出
                InputStream stdout = new StreamGobbler(sess.getStdout());
                BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
                while (true) {
                    String line = br.readLine();
                    if (line == null) {
                        break;
                    } else {
                        //首行统计信息输出是两个空格
                        if (!"".equals(line) && !line.contains("index")) {
                            fileList.add(line);
                        }
                    }
                }
                sess.close();
            } else {
                LOG.info(connection.getHostname() + " 认证失败!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
        return fileList;
    }

    public String processStdout(InputStream in, String charset) {
        byte[] buf = new byte[1024];
        StringBuilder sb = new StringBuilder();
        try {
            while (in.read(buf) != -1) {
                sb.append(new String(buf, charset));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}
