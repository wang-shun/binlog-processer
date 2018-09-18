package com.datatrees.datacenter.utility;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class HBaseHelper implements Serializable {
    private static Logger LOG = Logger.getLogger(HBaseHelper.class);
    private static Configuration innerHBaseConf = null;
    private static Connection innerHBaseConnection = null;

    public HBaseHelper() {
        initHBaseConfiguration();
        LOG.info("init configuration is successful");
        initHBaseConnection();
        LOG.info("init connection is successful");
    }

    /**
     * 内部方法，用来初始化HBaseConfiguration
     */
    private static void initHBaseConfiguration() {
        try {
            innerHBaseConf = HBaseConfiguration.create();
            File resourceFile = PropertiesUtility.loadResourceFile("hbase-site.xml");
            innerHBaseConf.addResource(resourceFile.getPath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HBaseConfiguration
     *
     * @return 返回HBaseConfiguration对象
     */
    public static Configuration getHBaseConfiguration() {
        if (null == innerHBaseConf) {
            initHBaseConfiguration();
        }
        return innerHBaseConf;
    }

    /**
     * 内部方法，用来初始化HBaseConnection
     */
    private static void initHBaseConnection() {
        try {
            innerHBaseConnection = ConnectionFactory.createConnection(HBaseHelper.getHBaseConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HBase连接
     *
     * @return 返回HBaseConnection对象
     */
    public static Connection getHBaseConnection() {
        if (null == innerHBaseConnection || innerHBaseConnection.isClosed()) {
            LOG.info("The HBaseConnection is null or closed, recreate connection");
            initHBaseConnection();
        }
        return innerHBaseConnection;
    }


    /**
     * 删除表格
     *
     * @param name 表名称
     */
    public void dropTable(String name) throws IOException {
        dropTable(TableName.valueOf(name));
    }

    /**
     * 内部方法：删除表格
     *
     * @param name 表名称
     */
    private void dropTable(TableName name) {
        Admin admin = null;
        try {
            admin = HBaseHelper.getHBaseConnection().getAdmin();
            boolean flag = admin.tableExists(name);
            if (flag) {
                if (admin.isTableEnabled(name)) {
                    admin.disableTable(name);
                }
                admin.deleteTable(name);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取表对象
     *
     * @param tableName 表名称
     * @return 表对象
     */
    public static Table getTable(String tableName) {
        if (null != tableName && tableName.length() > 0) {
            try {
                return HBaseHelper.getHBaseConnection().getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                LOG.info("Table does not exists");
            }
        }
        return null;
    }

    public static boolean putData(String tableName,
                                  String rowKey,
                                  String family,
                                  List<Map<String, byte[]>> kv) {
        Table table;
        try {
            table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map<String, byte[]> map : kv) {
                for (Map.Entry<String, byte[]> entry : map.entrySet()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), entry.getValue());
                }
            }
            table.put(put);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 关闭connection连接
     */
    public static void closeConn() {
        if (null != innerHBaseConnection && !innerHBaseConnection.isClosed()) {
            try {
                innerHBaseConnection.close();
                LOG.info("HBaseConnection close successfull");
            } catch (IOException e) {
                LOG.info("HBaseConnection close failed ");
                e.printStackTrace();
            }
        }
    }
}