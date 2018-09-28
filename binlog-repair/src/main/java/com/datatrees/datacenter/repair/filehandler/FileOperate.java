package com.datatrees.datacenter.repair.filehandler;

import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FileOperate {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private static final Properties PROPERTIES = PropertiesUtility.defaultProperties();
    private static final String AVRO_PATH = PROPERTIES.getProperty("AVRO_HDFS_PATH");

    InputStream getHdfsFileInput(String filePath) {
        InputStream is;
        FileSystem fs = HDFSFileUtility.getFileSystem(AVRO_PATH);
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
}
