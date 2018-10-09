package com.datatrees.datacenter.repair.filehandler;

import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.IpMatchUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FileOperate {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private static final Properties PROPERTIES = PropertiesUtility.defaultProperties();
    private static final String AVRO_HDFS_PATH = PROPERTIES.getProperty("AVRO_HDFS_PATH");

    public static InputStream getHdfsFileInputStream(String filePath) {
        InputStream is;
        FileSystem fs = HDFSFileUtility.getFileSystem(AVRO_HDFS_PATH);
        try {
            if (null != fs) {
                is = fs.open(new Path(filePath));
                return is;
            }
        } catch (IOException e) {
            LOG.info("File :" + filePath + " doesn't exist");
        }
        return null;
    }
    public static String getFilePath(String dataBase, String dbInstance, String tableName, String partition, String partitionType, String fileName) {
        StringBuilder filePathBuilder = new StringBuilder();
        if (IpMatchUtility.isboolIp(dbInstance)) {
            filePathBuilder
                    .append(AVRO_HDFS_PATH)
                    .append(File.separator)
                    .append(partitionType)
                    .append(File.separator)
                    .append(dataBase)
                    .append(File.separator)
                    .append(tableName)
                    .append(File.separator)
                    .append(partition)
                    .append(File.separator)
                    .append(fileName)
                    .append(".avro");
        } else {
            /**
             * idc AVRO_HDFS_PATH 为warehouse_idc
             * aliyun AVRO_HDFS_PATH 为warehouse
             * 配置文件中只配置warehouse_idc
             */
            filePathBuilder
                    .append(AVRO_HDFS_PATH.split("_")[0])
                    .append(File.separator)
                    .append(partitionType)
                    .append(File.separator)
                    .append(dbInstance)
                    .append(File.separator)
                    .append(dataBase)
                    .append(File.separator)
                    .append(tableName)
                    .append(File.separator)
                    .append(partition)
                    .append(File.separator)
                    .append(fileName)
                    .append(".avro");
        }
        return filePathBuilder.toString();
    }
}
