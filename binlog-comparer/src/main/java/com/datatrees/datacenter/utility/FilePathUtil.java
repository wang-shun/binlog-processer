package com.datatrees.datacenter.utility;

import com.datatrees.datacenter.core.utility.IpMatchUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.table.CheckTable;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilePathUtil {
    private static final String AVRO_HDFS_PATH = PropertiesUtility.defaultProperties().getProperty("AVRO_HDFS_PATH");

    public static String assembleFilePath(String dataBase, String tableName, String fileName, String partition, String dbInstance, String type) {
        String filePath;
        String idcHdfsPath = AVRO_HDFS_PATH + File.separator + type;
        String aliyunHdfsPath = AVRO_HDFS_PATH.split("_")[0] + File.separator + type;
        String filePathWithoutDbInstance = dataBase +
                File.separator +
                tableName +
                File.separator +
                partition +
                File.separator +
                fileName +
                CheckTable.FILE_LAST_NAME;
        if (IpMatchUtility.isboolIp(dbInstance)) {
            filePath = idcHdfsPath + File.separator + filePathWithoutDbInstance;
        } else {
            filePath = aliyunHdfsPath + File.separator + dbInstance + File.separator + filePathWithoutDbInstance;
        }
        return filePath;
    }

    private List<String> assembleRowKey(String dataBase, String tableName, Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            return recordMap.keySet().stream().map(x -> (x + "_" + dataBase + "." + tableName)).collect(Collectors.toList());
        } else {
            return null;
        }
    }
}
