package com.datatrees.datacenter.transfer.utility;

import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.*;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceAttributeResponse.DBInstanceAttribute;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse.NodeInfo;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import com.aliyuncs.rds.model.v20140815.DescribeDatabasesResponse.Database;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.process.AliYunConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author personalc
 */
public class DBInstanceUtil {
    private static Logger LOG = LoggerFactory.getLogger(DBInstance.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final int PAGE_SIZE = Integer.parseInt(properties.getProperty("PAGE_SIZE"));
    private static final String DBINSTANCE_LIST = properties.getProperty("DBINSTANCE_LIST");
    private static IAcsClient client = AliYunConfig.getClient();
    private static DefaultProfile profile = AliYunConfig.getProfile();
    private static String instanceSeparator = ",";
    private static String primaryInstanceFlag = "Primary";
    private static String slaveInstanceFlag = "Slave";

    /**
     * 获取所有实例id
     *
     * @return List<String>
     */
    public static List<String> getAllPrimaryInstanceId() {
        List<String> instanceIds = new ArrayList<>();
        if (DBINSTANCE_LIST != null && DBINSTANCE_LIST.length() > 0) {
            if (DBINSTANCE_LIST.contains(instanceSeparator)) {
                instanceIds = Arrays.asList(DBINSTANCE_LIST.split(instanceSeparator));
            } else {
                LOG.info("the only dbintance id is: " + DBINSTANCE_LIST);
                instanceIds.add(DBINSTANCE_LIST);
            }
        } else {
            DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
            DescribeDBInstancesResponse dbInstancesResponse;
            dbInstancesRequest.setDBInstanceType(primaryInstanceFlag);
            try {
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                int totalInstance = dbInstancesResponse.getTotalRecordCount();
                int pageCount = 0;
                if (totalInstance > 0) {
                    pageCount = (int) Math.ceil((double)totalInstance / PAGE_SIZE);
                }
                for (int i = 1; i <= pageCount; i++) {
                    dbInstancesRequest.setPageNumber(i);
                    dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                    List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                    for (DBInstance dbInstance : dbInstanceList) {
                        instanceIds.add(dbInstance.getDBInstanceId());
                    }
                }
            } catch (ClientException e) {
                LOG.error("can't get all primary instance");
            }
        }
        return instanceIds;
    }

    /**
     * 获取所有Mysql数据库实例（DBInstance）
     *
     * @return 返回所有的实例
     */
    public static List<DBInstance> getAllDBInstance() {
        List<String> instanceIds = new ArrayList<>();
        boolean flag = false;
        if (DBINSTANCE_LIST != null && DBINSTANCE_LIST.length() > 0) {
            if (DBINSTANCE_LIST.contains(instanceSeparator)) {
                instanceIds = Arrays.asList(DBINSTANCE_LIST.split(instanceSeparator));
            } else {
                LOG.info("the only dbintance id is: " + DBINSTANCE_LIST);
                instanceIds.add(DBINSTANCE_LIST);
            }
            flag = true;
        }
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
        DescribeDBInstancesResponse dbInstancesResponse;
        List<DBInstance> dbInstances = null;
        dbInstancesRequest.setDBInstanceType(primaryInstanceFlag);
        try {
            dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
            int totalInstance = dbInstancesResponse.getTotalRecordCount();
            dbInstances = new ArrayList<>(totalInstance);
            int pageCount = 0;
            if (totalInstance > 0) {
                pageCount = (int) Math.ceil((double)totalInstance / PAGE_SIZE);
            }
            LOG.info("pageCount: " + pageCount);
            for (int i = 1; i <= pageCount; i++) {
                dbInstancesRequest.setPageNumber(i);
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                Iterator<DBInstance> iterator = dbInstanceList.iterator();
                if (flag) {
                    while (iterator.hasNext()) {
                        DBInstance dbInstance = iterator.next();
                        if (!instanceIds.contains(dbInstance.getDBInstanceId())) {
                            iterator.remove();
                        }
                    }
                }
                dbInstances.addAll(dbInstanceList);
            }
        } catch (ClientException e) {
            LOG.error("can't get all primary instance");
        }
        return dbInstances;
    }

    /**
     * @return
     */
    public static List<DBInstance> getAllPrimaryDBInstance() {
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
        DescribeDBInstancesResponse dbInstancesResponse;
        List<DBInstance> dbInstances = null;
        dbInstancesRequest.setDBInstanceType(primaryInstanceFlag);
        try {
            dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
            int totalInstance = dbInstancesResponse.getTotalRecordCount();
            dbInstances = new ArrayList<>(totalInstance);
            int pageCount = 0;
            if (totalInstance > 0) {
                pageCount = (int) Math.ceil((double) totalInstance / PAGE_SIZE);
            }
            LOG.info("pageCount: " + pageCount);
            for (int i = 1; i <= pageCount; i++) {
                dbInstancesRequest.setPageNumber(i);
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                dbInstances.addAll(dbInstanceList);
            }
        } catch (ClientException e) {
            LOG.error("can't get all primary instance");
        }
        return dbInstances;
    }

    /**
     * 获取实例的备份实例编号
     *
     * @param instanceId 某个实例
     * @return 备份实例编号
     */
    public static String getBackInstanceId(String instanceId) {
        DescribeDBInstanceHAConfigRequest haConfigRequest = new DescribeDBInstanceHAConfigRequest();
        haConfigRequest.setActionName("DescribeDBInstanceHAConfig");
        haConfigRequest.setDBInstanceId(instanceId);
        String backInstanceId = null;
        try {
            DescribeDBInstanceHAConfigResponse haConfigResponse = client.getAcsResponse(haConfigRequest, profile);
            List<NodeInfo> hostInstanceInfos = haConfigResponse.getHostInstanceInfos();
            for (NodeInfo hostInstanceInfo : hostInstanceInfos) {
                if (slaveInstanceFlag.equals(hostInstanceInfo.getNodeType())) {
                    backInstanceId = hostInstanceInfo.getNodeId();
                }
            }

        } catch (ClientException e) {
            LOG.error("can't get the slave of instance : " + instanceId + " , please check the instance id");
        }

        return backInstanceId;
    }

    /**
     * 返回所有实例拼接字符串
     */
    public static String getInstancesString(List<String> instanceIds) {
        StringBuilder instanceStr = new StringBuilder();
        for (int i = 0; i < instanceIds.size(); i++) {
            if (i < instanceIds.size() - 1) {
                instanceStr.append("'").append(instanceIds.get(i)).append("'").append(",");
            } else {
                instanceStr.append("'").append(instanceIds.get(i)).append("'");
            }
        }
        return instanceStr.toString();
    }

    /**
     * 返回实例下的所有数据库列表
     *
     * @param dbInstance 实例
     * @return 数据库列表
     */
    public static List<Database> getDataBase(DBInstance dbInstance) {
        DescribeDatabasesRequest databasesRequest = new DescribeDatabasesRequest();
        databasesRequest.setActionName("DescribeDatabases");
        databasesRequest.setDBInstanceId(dbInstance.getDBInstanceId());
        List<Database> databases = null;
        try {
            DescribeDatabasesResponse response = client.getAcsResponse(databasesRequest, profile);
            databases = response.getDatabases();
        } catch (ClientException e) {
            LOG.error("can't get the databases of instance : " + dbInstance.getDBInstanceId());
        }
        return databases;
    }

    /**
     * 将实例下的所有数据库名拼接为字符串
     *
     * @param databases
     * @return
     */
    public static String dataBasesToStr(List<Database> databases) {
        StringBuilder dataBaseNames = new StringBuilder();
        for (Database database : databases) {
            dataBaseNames.append(database.getDBName()).append("=");
        }
        return dataBaseNames.toString();
    }

    /**
     * 获取实例内网地址
     *
     * @param instanceId
     * @return
     */
    public static String getConnectString(String instanceId) {
        DescribeDBInstanceAttributeRequest attributeRequest = new DescribeDBInstanceAttributeRequest();
        attributeRequest.setActionName("DescribeDBInstanceAttribute");
        attributeRequest.setDBInstanceId(instanceId);
        List<DBInstanceAttribute> dbInstanceAttributeList;
        String connectString = null;
        try {
            DescribeDBInstanceAttributeResponse response = client.getAcsResponse(attributeRequest, profile);
            dbInstanceAttributeList = response.getItems();

            for (DBInstanceAttribute attribute : dbInstanceAttributeList) {
                if (attribute.getDBInstanceId().equals(instanceId)) {
                    connectString = attribute.getConnectionString();
                }
            }
        } catch (ClientException e) {
            LOG.error("can't get the connection string of instance : " + instanceId);
        }
        return connectString;
    }
}

