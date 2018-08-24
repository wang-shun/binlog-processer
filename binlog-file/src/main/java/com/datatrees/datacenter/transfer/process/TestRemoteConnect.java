package com.datatrees.datacenter.transfer.process;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import sun.management.FileSystem;

public class TestRemoteConnect
{

    public static void main(String[] args)
    {
        String hostname = "xxx.xxx.xxx.xxx";
        int port = 22;
        String username = "xxx";
        String password = "xxx";

        Connection conn = new Connection(hostname,port);
        Session ssh = null;
        try
        {
            //连接到主机
            conn.connect();
            //使用用户名和密码校验
            boolean isconn = conn.authenticateWithPassword(username, password);
            if (!isconn)
            {
                System.out.println("用户名称或者是密码不正确");
            }
            else
            {
                System.out.println("已经连接OK");
                File folder = new File("D://logs");
                if(!folder.exists()){
                    folder.mkdir();
                }

                SCPClient clt = conn.createSCPClient();
                ssh = conn.openSession();
                ssh.execCommand("find /dev/shm/M/ -name '*.txt'");
                InputStream is = new StreamGobbler(ssh.getStdout());
                BufferedReader brs = new BufferedReader(new InputStreamReader(is));
                while (true)
                {
                    String line = brs.readLine();
                    if (line == null)
                    {
                        break;
                    }
                    clt.get( "D://logs"+File.separator+line);
                }

            }
        }
        catch (IOException e)
        {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally
        {
            //连接的Session和Connection对象都需要关闭
            if(ssh!=null)
            {
                ssh.close();
            }
            if(conn!=null)
            {
                conn.close();
            }
        }
    }

    public List<File> getFileList(File fileDir,String fileType){
        List<File> lfile = new ArrayList<File>();
        File[] fs = fileDir.listFiles();
        for(File f:fs){
            if(f.isFile()){
                if (fileType.equals(f.getName().substring(f.getName().lastIndexOf(".")+1, f.getName().length()))) {
                    lfile.add(f);
                }
            }else{
                List<File> ftemps = getFileList(f, fileType);
                lfile.addAll(ftemps);
            }
        }
        return lfile;
    }
}

