package com.datatrees.datacenter.transfer.utility;

import com.datatrees.datacenter.transfer.bean.SshConfiguration;
import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Properties;

public class SshUtil {
    private ChannelSftp channelSftp;
    private ChannelExec channelExec;
    private Session session = null;
    private int timeout = 60000;
    private static Logger LOG = LoggerFactory.getLogger(FileUtil.class);


    public SshUtil(SshConfiguration conf) throws JSchException {
        LOG.info("try connect to  " + conf.getHost() + ",username: " + conf.getUserName() + ",password: " + conf.getPassword() + ",port: " + conf.getPort());
        //创建JSch对象
        JSch jSch = new JSch();
        //根据用户名，主机ip和端口获取一个Session对象
        session = jSch.getSession(conf.getUserName(), conf.getHost(), conf.getPort());
        //设置密码
        session.setPassword(conf.getPassword());
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        //为Session对象设置properties
        session.setConfig(config);
        //设置超时
        session.setTimeout(timeout);
        //通过Session建立连接
        session.connect();
    }

    public void download(String src, String dst) throws JSchException, SftpException {
        //src linux服务器文件地址，dst 本地存放地址
        channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.get(src, dst);
        channelSftp.quit();
    }

    public void upLoad(String src, String dst) throws JSchException, SftpException {
        //src 本机文件地址。 dst 远程文件地址
        channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.put(src, dst);
        channelSftp.quit();

    }

    public void close() {
        session.disconnect();
    }

    public static void main(String[] args) {
        SshConfiguration configuration = new SshConfiguration();
        configuration.setHost("172.17.1.232");
        configuration.setUserName("root");
        configuration.setPassword("root275858");
        configuration.setPort(22);
        try {
//            SshUtil sshUtil=new SshUtil(configuration);
//            sshUtil.download("/home/cafintech/Logs/metaData/meta.log","D://meta.log");
//            sshUtil.close();
//            System.out.println("文件下载完成");

            SshUtil sshUtil = new SshUtil(configuration);
            sshUtil.upLoad("D://meta.log", "/home/cafintech/");
            sshUtil.close();
            System.out.println("文件上传完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
