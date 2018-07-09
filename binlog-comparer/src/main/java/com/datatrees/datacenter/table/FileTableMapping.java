package com.datatrees.datacenter.table;

import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FileTableMapping {
    static final Properties properties = PropertiesUtility.defaultProperties();
    static final String HDFS_PATH = properties.getProperty("HDFS_PATH");

    public static Map<String, Object> getFileInfo(String identity) {
        String[] info = identity.split("_");
        if (info.length > 1) {
            String dbInstance = info[0];
            String fileName = info[1];
        }
        return null;
    }
}
