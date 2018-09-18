package com.datatrees.datacenter.transfer.bean;

import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.Properties;

public class LocalCenterInfo {
    public static Properties properties = PropertiesUtility.defaultProperties();
    public static final String[] Ips = properties.getProperty("SERVER_IP").split(",");
    public static final int PORT = Integer.valueOf(properties.getProperty("PORT", "22"));
    public static final String DATABASE = properties.getProperty("jdbc.database", "binlog");
    public static final String SERVER_BASEDIR = properties.getProperty("SERVER_ROOT", "/data1/application/binlog-process/log");
    public static final String CLIENT_BASEDIR = properties.getProperty("CLIENT_ROOT", "/Users/personalc/test");
    public static final String HDFS_PATH = properties.getProperty("HDFS_PATH");
}
