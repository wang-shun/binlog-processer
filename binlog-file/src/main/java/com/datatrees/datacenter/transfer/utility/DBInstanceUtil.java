package com.datatrees.datacenter.transfer.utility;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.*;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author personalc
 */
public class DBInstanceUtil {
    private static Logger LOG = LoggerFactory.getLogger(DBInstance.class);
    private static Properties properties = FileUtil.getProperties();
    private static final int PAGE_SIZE = Integer.valueOf(properties.getProperty("PAGE_SIZE"));
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final DefaultProfile DEFAULT_PROFILE;
    private static IAcsClient client;

    static {
        DEFAULT_PROFILE = DefaultProfile.getProfile(
                // 您的可用区ID
                REGION_ID,
                // 您的AccessKey ID
                ACCESS_KEY_ID,
                // 您的AccessKey Secret
                ACCESS_SECRET);
    }

    /**
     * 云平台连接设置
     *
     * @return IAcsClient实例
     */
    private static IAcsClient createConnection() {
        if (null == client) {
            client = new DefaultAcsClient(DEFAULT_PROFILE);
        }
        return client;

    }

    /**
     * 获取所有Mysql数据库实例（DBInstance）
     *
     * @return 返回所有的实例
     */
    public static List<DBInstance> getAllPrimaryDBInstance() {
        IAcsClient client = createConnection();
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
        DescribeDBInstancesResponse dbInstancesResponse;
        List<DBInstance> dbInstances = null;
        dbInstancesRequest.setDBInstanceType("Primary");
        try {
            dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, DEFAULT_PROFILE);
            int totalInstance = dbInstancesResponse.getTotalRecordCount();
            dbInstances = new ArrayList<>(totalInstance);
            int pageCount = 0;
            if (totalInstance > 0) {
                pageCount = (int) Math.ceil(totalInstance / PAGE_SIZE);
            }
            LOG.info("pageCount: " + pageCount);
            for (int i = 1; i <= pageCount; i++) {
                dbInstancesRequest.setPageNumber(i);
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, DEFAULT_PROFILE);
                List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                for (DBInstance dbInstance : dbInstanceList) {
                    System.out.println(dbInstance.getDBInstanceId());
                }
                dbInstances.addAll(dbInstanceList);
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return dbInstances;
    }

    /**
     * 获取实例的备份实例编号
     *
     * @param dbInstance 某个实例
     * @return 备份实例编号
     */
    public static String getBackInstanceId(DBInstance dbInstance) {
        IAcsClient client = DBInstanceUtil.createConnection();
        DescribeDBInstanceHAConfigRequest haConfigRequest = new DescribeDBInstanceHAConfigRequest();
        String instanceId = dbInstance.getDBInstanceId();
        haConfigRequest.setActionName("DescribeDBInstanceHAConfig");
        haConfigRequest.setDBInstanceId(instanceId);
        String backInstanceId = null;
        try {
            DescribeDBInstanceHAConfigResponse haConfigResponse = client.getAcsResponse(haConfigRequest, DBInstanceUtil.getProfile());
            List<DescribeDBInstanceHAConfigResponse.NodeInfo> hostInstanceInfos = haConfigResponse.getHostInstanceInfos();
            for (DescribeDBInstanceHAConfigResponse.NodeInfo hostInstanceInfo : hostInstanceInfos) {
                if ("Slave".equals(hostInstanceInfo.getNodeType())) {
                    backInstanceId = hostInstanceInfo.getNodeId();
                }
            }

        } catch (ClientException e) {
            e.printStackTrace();
        }

        return backInstanceId;
    }

    /**
     * 返回实例下的所有数据库列表
     *
     * @param dbInstance 实例
     * @return 数据库列表
     */
    public static List<DescribeDatabasesResponse.Database> getDataBase(DBInstance dbInstance) {
        DescribeDatabasesRequest databasesRequest = new DescribeDatabasesRequest();
        databasesRequest.setActionName("DescribeDatabases");
        databasesRequest.setDBInstanceId(dbInstance.getDBInstanceId());
        List<DescribeDatabasesResponse.Database> databases = null;
        try {
            DescribeDatabasesResponse response = client.getAcsResponse(databasesRequest, DEFAULT_PROFILE);
            databases = response.getDatabases();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return databases;
    }

    /**
     * 将实例下的所有数据库名拼接为字符串
     *
     * @param databases
     * @return
     */
    public static String dataBasesToStr(List<DescribeDatabasesResponse.Database> databases) {
        StringBuilder dataBaseNames = new StringBuilder();
        for (int i = 0; i < databases.size(); i++) {
            dataBaseNames.append(databases.get(i).getDBName()).append("=");
        }
        return dataBaseNames.toString();
    }

    /**
     * 获取实例内网地址
     *
     * @param dbInstance
     * @return
     */
    public static String getConnectString(DBInstance dbInstance) {
        DescribeDBInstanceAttributeRequest attributeRequest = new DescribeDBInstanceAttributeRequest();
        attributeRequest.setActionName("DescribeDBInstanceAttribute");
        attributeRequest.setDBInstanceId(dbInstance.getDBInstanceId());
        List<DescribeDBInstanceAttributeResponse.DBInstanceAttribute> databases = null;
        String connectString = null;
        try {
            DescribeDBInstanceAttributeResponse response = client.getAcsResponse(attributeRequest, DEFAULT_PROFILE);
            List<DescribeDBInstanceAttributeResponse.DBInstanceAttribute> dbInstanceAttributeList = response.getItems();

            for (int i = 0; i < dbInstanceAttributeList.size(); i++) {
                DescribeDBInstanceAttributeResponse.DBInstanceAttribute attribute = dbInstanceAttributeList.get(i);
                if (attribute.getDBInstanceId().equals(dbInstance.getDBInstanceId())) {
                    connectString = attribute.getConnectionString();
                }
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return connectString;
    }

    private static DefaultProfile getProfile() {
        return DEFAULT_PROFILE;
    }
}

