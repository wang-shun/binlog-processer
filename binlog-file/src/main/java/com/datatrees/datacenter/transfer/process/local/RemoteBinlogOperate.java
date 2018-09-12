package com.datatrees.datacenter.transfer.process.local;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.transfer.bean.LocalBinlogInfo;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.sql.SQLException;
import java.util.*;

public class RemoteBinlogOperate implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(RemoteBinlogOperate.class);
    public static Properties properties = PropertiesUtility.defaultProperties();
    private static final int PORT = Integer.valueOf(properties.getProperty("PORT", "22"));
    private static final String USER = properties.getProperty("USER_NAME", "root");
    private static final String PASSWORD = properties.getProperty("PASSWORD", "");
    private static final String DATABASE = properties.getProperty("jdbc.database", "binlog");
    private static final String SERVER_BASEDIR = properties.getProperty("SERVER_ROOT", "/data1/application/binlog-process/log");
    private static final String CLIENT_BASEDIR = properties.getProperty("CLIENT_ROOT", "/Users/personalc/test/");
    private static final String HDFS_PATH = properties.getProperty("HDFS_ROOT");

    private String hostIp;
    private static Map<String, String> hostFileMap = getHostFileMap();
    private boolean recordExist = false;

    /**
     * 本机的私钥文件
     */
    private static String PRIVATEKEY = properties.getProperty("PRIVATE_KEY", "/Users/personalc/.ssh/id_rsa");
    /**
     * 使用用户名和密码来进行登录验证。如果为true则通过用户名和密码登录，false则使用rsa免密码登录
     */
    private static boolean usePassword = properties.getProperty("USE_PASSWORD").equals("false") ? false : true;

    /**
     * ssh用户登录验证，使用用户名和密码来认证
     *
     * @param user
     * @param password
     * @return
     */
    public static boolean isAuthedWithPassword(String user, String password, Connection connection) {
        try {
            return connection.authenticateWithPassword(user, password);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false;
    }

    /**
     * ssh用户登录验证，使用用户名、私钥、密码来认证 其中密码如果没有可以为null，生成私钥的时候如果没有输入密码，则密码参数为null
     *
     * @param user
     * @param privateKey
     * @param password
     * @return
     */
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

    private static void getFile(String remoteFile, String localDirectory, Connection connection) {
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
                    boolean flag = localPath.mkdir();
                    if (flag) {
                        LOG.info("文件夹：" + localDirectory + " 创建成功");
                    } else {
                        LOG.error("文件夹：" + localDirectory + " 创建失败");
                    }
                }
                scpClient.get(remoteFile, localDirectory);
                Log.info("文件:" + remoteFile + " 下载完毕");
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
                Log.info("从主机: [" + connection.getHostname() + "] 下载文件:" + Arrays.toString(remoteFiles) + " 下载完毕");
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

    private static List<String> getFileList(String filePath, Connection connection) {
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
                Log.info("文件信息：" + fileList.toString());
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

    public static Map<String, String> getHostFileMap() {
        Map<String, String> hostFileMap = new HashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select ");
        stringBuilder.append(LocalBinlogInfo.dbInstance);
        stringBuilder.append(",");
        stringBuilder.append(LocalBinlogInfo.fileName);
        stringBuilder.append(" from ");
        stringBuilder.append(LocalBinlogInfo.binlogDownTable);
        List<Map<String, Object>> dataRecord = null;
        try {
            dataRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), DATABASE, stringBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (dataRecord != null) {
            dataRecord.stream().forEach(x -> hostFileMap.put(String.valueOf(x.get(LocalBinlogInfo.dbInstance)), String.valueOf(x.get(LocalBinlogInfo.fileName))));
        }
        return hostFileMap;
    }


    @Override
    public void run() {
        try {
            Connection connection = new Connection(hostIp, PORT);
            List<String> fileList = getFileList(SERVER_BASEDIR, connection);
            if (null != fileList && fileList.size() > 1) {
                // TODO: 2018/9/7 暂时取IP地址最后一部分作为dbInstance
                String ipStr = hostIp.split("\\.")[3];
                List<String> subFileList = null;
                long batchStart = System.currentTimeMillis();
                if (null != hostFileMap && hostFileMap.size() > 0) {
                    String lastFileName = hostFileMap.get(hostIp);
                    if (lastFileName != null) {
                        LOG.info("the last download binlog file of :" + hostIp + " is :" + lastFileName);
                        int lastIndex = fileList.indexOf(lastFileName);
                        System.out.println("lastIndex:" + lastIndex);
                        if (fileList.size() - 1 > lastIndex) {
                            subFileList = fileList.subList(lastIndex + 1, fileList.size() - 1);
                            recordExist = true;
                        }
                        if (lastIndex == -1) {
                            if (fileList.size() > 1) {
                                subFileList = fileList.subList(0, fileList.size() - 1);
                            }
                        }
                    } else {
                        Log.info("no binlog download record find in the database");
                        if (fileList.size() > 1) {
                            subFileList = fileList.subList(0, fileList.size() - 1);
                        }
                    }
                } else {
                    if (fileList.size() > 1) {
                        subFileList = fileList.subList(0, fileList.size() - 1);
                    }
                }
                if (null != subFileList && subFileList.size() > 0) {
                    LOG.info("the binlog files need to download : " + subFileList.toString());

                    Map<String, Object> valueMap = new HashMap<>(6);
                    valueMap.put(TableInfo.DB_INSTANCE, ipStr);
                    valueMap.put(TableInfo.HOST, IPUtility.ipAddress());
                    valueMap.put(TableInfo.BATCH_ID, TimeUtil.timeStamp2DateStr(batchStart, "yyyy-MM-dd HH:mm:ss"));
                    Map<String, Object> lastValueMap = new HashMap<>(3);
                    lastValueMap.put(LocalBinlogInfo.downloadIp, IPUtility.ipAddress());
                    Map<String, Object> whereMap = new HashMap<>(1);
                    whereMap.put(LocalBinlogInfo.dbInstance, hostIp);
                    String hdfsFilePath = HDFS_PATH + File.separator + ipStr;
                    for (int i = 0; i < subFileList.size(); i++) {
                        String fileName = subFileList.get(i);
                        String remoteFilePath = SERVER_BASEDIR + fileName;
                        long downStart = System.currentTimeMillis();
                        getFile(remoteFilePath, CLIENT_BASEDIR + ipStr, connection);
                        long downEnd = System.currentTimeMillis();
                        valueMap.put(TableInfo.DOWN_START_TIME, TimeUtil.stampToDate(downStart));
                        valueMap.put(TableInfo.DOWN_END_TIME, TimeUtil.stampToDate(downEnd));
                        String localFilePath = CLIENT_BASEDIR + ipStr + File.separator + fileName;
                        File localFile = new File(localFilePath);
                        if (localFile.isFile() && localFile.exists()) {
                            Boolean uploadFlag = HDFSFileUtility.put2HDFS(localFilePath, hdfsFilePath, HDFSFileUtility.conf);
                            if (uploadFlag) {
                                LOG.info("file ：" + fileName + " upload to HDFS successful！");

                                valueMap.put(TableInfo.FILE_NAME, fileName);
                                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), "binlog", "t_binlog_record_copy", valueMap);

                                lastValueMap.put(LocalBinlogInfo.fileName, fileName);
                                long uploadTime = System.currentTimeMillis();
                                lastValueMap.put(LocalBinlogInfo.downloadTime, TimeUtil.stampToDate(uploadTime));
                                if (!recordExist && i == 0) {
                                    lastValueMap.put(LocalBinlogInfo.dbInstance, hostIp);
                                    DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), "binlog", "t_binlog_down_last_file", lastValueMap);
                                } else {
                                    DBUtil.update(DBServer.DBServerType.MYSQL.toString(), "binlog", "t_binlog_down_last_file", lastValueMap, whereMap);
                                }
                                // TODO: 2018/9/10 发送至消息队列
                        /*String filePath = path + fileName;
                        TaskDispensor.defaultDispensor().dispense(new Binlog(filePath, hostIp + "_" + fileName, ""));*/
                            } else {
                                LOG.info("file ：" + fileName + "upload to HDFS failed！");
                            }
                        } else {
                            LOG.info("File:" + localFile + " does not exist!");
                        }
                    }
                }
            } else {
                LOG.info("no binlog find in the host : " + hostIp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }
}

