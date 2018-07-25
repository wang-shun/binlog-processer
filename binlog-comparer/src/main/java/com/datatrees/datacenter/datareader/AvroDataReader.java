package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class AvroDataReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private final Properties properties = PropertiesUtility.defaultProperties();
    private final String avroPath = properties.getProperty("AVRO_HDFS_PATH");
    private String dataBase;
    private String tableName;
    private String recordId;
    private String recordLastUpdateTime;

    @Override
    public Map<String, Map<String, Long>> readSrcData(String filePath) {
        InputStream is;
        Map<String, Map<String, Long>> recordMap = null;
        try {
            FileSystem fs = HDFSFileUtility.getFileSystem(avroPath);
            if (null != fs) {
                is = fs.open(new Path(filePath));
                if (null != is) {
                    recordMap = readFromAvro(is);
                }
            }
        } catch (IOException e) {
            LOG.info(e.getMessage());
        }
        return recordMap;
    }

    /**
     * 读取Avro文件中的记录并根据事件分类
     *
     * @param is 输入文件流
     * @return Avro中分类后的事件信息
     */
    private Map<String, Map<String, Long>> readFromAvro(InputStream is) {
        Map<String, Map<String, Long>> recordMap = new HashMap<>(2);
        if (null != recordId && null != recordLastUpdateTime) {
            Map<String, Long> uniqueMap = new HashMap<>();
            Map<String, Long> deleteMap = new HashMap<>();
            try {
                DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
                Iterator<Object> iterator = reader.iterator();
                while (iterator.hasNext()) {
                    Object o = iterator.next();
                    GenericRecord r = (GenericRecord) o;
                    String operator = r.get(2).toString();
                    JSONObject jsonObject;
                    if (null != r.get(1)) {
                        jsonObject = JSONObject.parseObject(r.get(1).toString());
                    } else {
                        jsonObject = JSONObject.parseObject(r.get(0).toString());
                    }
                    String id = String.valueOf(jsonObject.get(recordId));
                    long lastUpdateTime = jsonObject.getLong(recordLastUpdateTime);
                    switch (operator) {
                        case "Create":
                        case "Update":
                            uniqueMap.put(id, lastUpdateTime);
                            break;
                        case "Delete":
                            uniqueMap.put(id, lastUpdateTime);
                            deleteMap.put(id, lastUpdateTime);
                            break;
                        default:
                            break;
                    }
                }
                IOUtils.cleanup(null, is);
                IOUtils.cleanup(null, reader);

                recordMap.put(OperateType.Unique.toString(), uniqueMap);
                recordMap.put(OperateType.Delete.toString(), deleteMap);
                return recordMap;
            } catch (Exception e) {
                LOG.info("can't read the avro file");
                return null;
            }
        }
        return null;

    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRecordLastUpdateTime() {
        return recordLastUpdateTime;
    }

    public void setRecordLastUpdateTime(String recordLastUpdateTime) {
        this.recordLastUpdateTime = recordLastUpdateTime;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }
}
