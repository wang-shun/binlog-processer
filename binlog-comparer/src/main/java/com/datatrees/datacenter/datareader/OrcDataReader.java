package com.datatrees.datacenter.datareader;

import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.orc.StripeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author personalc
 */
public class OrcDataReader extends DataReader {
    private static Logger LOG = LoggerFactory.getLogger(OrcDataReader.class);

    @Override
    public Map<String, Long> readDestData(String filePath) {
        List<String> fileList = HDFSFileUtility.getFilesPath(filePath);
        Map<String, Long> opRecord = new HashMap<>();
        if (fileList.size() > 0) {
            for (String file : fileList) {
                opRecord.putAll(readOrcData(file));
            }
        } else {
            LOG.info("no file find in file Path:" + filePath);
        }
        return opRecord;
    }

    /**
     * @param filePath 文件路径
     * @return 记录ID和时间戳
     */
    private Map<String, Long> readOrcData(String filePath) {
        Map<String, Long> dataMap = null;
        try {
            Reader reader = OrcFile.createReader(HDFSFileUtility.fileSystem, new Path(filePath));
            StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
            RecordReader records = reader.rows();
            Object row = null;
            List fields = inspector.getAllStructFieldRefs();
            dataMap = new HashMap<>();
            while (records.hasNext()) {
                row = records.next(row);
                String id = inspector.getStructFieldData(row, (StructField) fields.get(0)).toString();
                String lastUpdateTime = inspector.getStructFieldData(row, (StructField) fields.get(fields.size() - 1)).toString();
                long lastTime = Timestamp.valueOf(lastUpdateTime).getTime();
                dataMap.put(id, lastTime);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return dataMap;
    }

    /**
     * 返回orc文件行数
     *
     * @param fs    HDFS files system
     * @param fName 文件名
     * @return 文件行数
     */
    private long getRowCount(FileSystem fs, String fName) {
        long tempCount = 0;
        Reader rdr = null;
        try {
            rdr = OrcFile.createReader(fs, new Path(fName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        StructObjectInspector insp = (StructObjectInspector) rdr.getObjectInspector();
        List<StripeInformation> iterable = rdr.getStripes();
        for (StripeInformation stripe : iterable) {
            tempCount = tempCount + stripe.getNumberOfRows();
        }
        return tempCount;
    }

}
