package com.datatrees.datacenter.datareader;

import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.table.FieldNameOp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;


/**
 * @author personalc
 */
public class OrcDataReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(OrcDataReader.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private final String hivePath = properties.getProperty("HIVE_HDFS_PATH");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> timeList = FieldNameOp.getConfigField("update");

    @Override
    public Map<String, Long> readDestData(String filePath) {
        List<String> fileList = HDFSFileUtility.getFilesPath(hivePath + File.separator + filePath);
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
            Reader reader = OrcFile.createReader(HDFSFileUtility.getFileSystem(hivePath), new Path(filePath));
            StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
            RecordReader records = reader.rows();
            Object row = null;
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            List<String> fieldList = new ArrayList<>(fields.size());
            for (StructField oneField : fields) {
                String colName = oneField.getFieldName();
                fieldList.add(colName);

            }
            dataMap = new HashMap<>();

            String idField = null;
            String timeField = null;
            if (idList.retainAll(fieldList)) {
                idField = idList.get(0);
            }
            if (timeList.retainAll(fieldList)) {
                timeField = timeList.get(0);
            }
            if (null != idField && null != timeField) {
                while (records.hasNext()) {
                    row = records.next(row);
                    String id = inspector.getStructFieldData(row, fields.get(fields.indexOf(idField))).toString();
                    String lastUpdateTime = inspector.getStructFieldData(row, fields.get(fields.indexOf(timeField))).toString();
                    long lastTime = Timestamp.valueOf(lastUpdateTime).getTime();
                    dataMap.put(id, lastTime);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return dataMap;
    }

}
