package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.zookeeper.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class AvroDataReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private final Properties properties = PropertiesUtility.defaultProperties();
    private final String avroPath = properties.getProperty("AVRO_HDFS_PATH");
    // TODO: 2018/8/1 注意删除绑定
    private String dataBase;
    private String tableName;
    private String recordId = "Id";
    private String recordLastUpdateTime;

    @Override
    public Map<String, Map<String, ?>> readSrcData(String filePath) {
        InputStream is;
        FileSystem fs = HDFSFileUtility.getFileSystem(avroPath);
        try {
            if (null != fs) {
                is = fs.open(new Path(filePath));
                if (null != is) {
                    return readFromAvro(is);
                }
            }
        } catch (IOException e) {
            LOG.info("file " + filePath + " doesn't exist");
        }
        return null;
    }

    /**
     * 读取Avro文件中的记录并根据事件分类
     *
     * @param is 输入文件流
     * @return Avro中分类后的事件信息
     */
    private Map<String, Map<String, ?>> readFromAvro(InputStream is) {
        Map<String, Map<String, ?>> recordMap = new HashMap<>(2);
        if (null != recordId && null != recordLastUpdateTime) {

            Map<String, Long> createMap = new HashMap<>();
            Map<String, Long> updateMap = new HashMap<>();
            Map<String, Long> uniqueMap = new HashMap<>();
            Map<String, Long> deleteMap = new HashMap<>();
            Map<String, String> dataMap = new HashMap<>();
            Object obj;
            GenericRecord r;
            String operator;
            JSONObject jsonObject;
            String id;
            long lastUpdateTime;

            try {
                DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
                Iterator<Object> iterator = reader.iterator();
                while (iterator.hasNext()) {
                    obj = iterator.next();
                    r = (GenericRecord) obj;
                    operator = r.get(2).toString();
                    switch (operator) {
                        case "Create":
                            jsonObject = JSONObject.parseObject(r.get(1).toString());
                            id = String.valueOf(jsonObject.get(recordId));
                            lastUpdateTime = jsonObject.getLong(recordLastUpdateTime);
                            createMap.put(id, lastUpdateTime);
                            uniqueMap.put(id, lastUpdateTime);
                            dataMap.put(id, jsonObject.toJSONString());
                            break;
                        case "Update":
                            jsonObject = JSONObject.parseObject(r.get(1).toString());
                            id = String.valueOf(jsonObject.get(recordId));
                            lastUpdateTime = jsonObject.getLong(recordLastUpdateTime);
                            updateMap.put(id,lastUpdateTime);
                            uniqueMap.put(id, lastUpdateTime);
                            dataMap.put(id, jsonObject.toJSONString());
                            break;
                        case "Delete":
                            jsonObject = JSONObject.parseObject(r.get(0).toString());
                            id = String.valueOf(jsonObject.get(recordId));
                            lastUpdateTime = jsonObject.getLong(recordLastUpdateTime);
                            uniqueMap.put(id, lastUpdateTime);
                            deleteMap.put(id, lastUpdateTime);
                            dataMap.put(id, jsonObject.toJSONString());
                            break;
                        default:
                            break;
                    }
                }
                IOUtils.cleanup(null, is);
                IOUtils.cleanup(null, reader);
                recordMap.put(OperateType.Create.toString(), BaseDataCompare.retainCompare(createMap, uniqueMap));
                recordMap.put(OperateType.Update.toString(), BaseDataCompare.retainCompare(updateMap, uniqueMap));
                recordMap.put(OperateType.Delete.toString(), deleteMap);
                recordMap.put(OperateType.Data.toString(), dataMap);
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
