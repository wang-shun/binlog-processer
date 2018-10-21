package com.datatrees.datacenter.main;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Vector;

public class SshTest {
    public static void main(String args[]){
        JSch jsch = new JSch();
        Session session = null;
        try {
            session = jsch.getSession("binlog","172.16.100.18",22);
            session.setPassword("Dashu0701");
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            ChannelSftp channel = (ChannelSftp)session.openChannel("sftp");
            channel.connect();
            //If you want you can change the directory using the following line.
            channel.cd("/app/mysql/log/binlog/");
            Vector<ChannelSftp.LsEntry> directoryEntries = channel.ls("/app/mysql/log/binlog/");
            for (ChannelSftp.LsEntry file : directoryEntries) {
                System.out.println(String.format("File - %s", file.getFilename()));
                if(!file.getAttrs().isDir()) {
                    File localFile = new File(file.getFilename());
                    channel.put(new FileInputStream(localFile), localFile.getName());
                }
            }
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }

    }
}
