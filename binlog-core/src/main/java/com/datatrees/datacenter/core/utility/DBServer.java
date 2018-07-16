package com.datatrees.datacenter.core.utility;

import java.util.Properties;

public class DBServer {
    private static Properties properties = PropertiesUtility.defaultProperties();

    public enum DBServerType {
        MYSQL,
        TIDB
    }

    public static DBConnectionPool.DBInfo getDBInfo(String dbServer) {
        DBConnectionPool.DBInfo dbInfo = new DBConnectionPool.DBInfo();
        String driverClassName;
        String url;
        String username;
        String password;
        if (dbServer.equalsIgnoreCase(DBServerType.MYSQL.toString())) {
            driverClassName = properties.getProperty("jdbc.driverClassName");
            url = properties.getProperty("jdbc.url");
            username = properties.getProperty("jdbc.username");
            password = properties.getProperty("jdbc.password");
        } else {
            driverClassName = properties.getProperty("tidb.driverClassName");
            url = properties.getProperty("tidb.url");
            username = properties.getProperty("tidb.username");
            password = properties.getProperty("tidb.password");
        }
        dbInfo.setDriver(driverClassName);
        dbInfo.setUrl(url);
        dbInfo.setUsername(username);
        dbInfo.setPassword(password);
        return dbInfo;
    }
}
