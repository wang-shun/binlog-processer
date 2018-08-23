package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import javafx.beans.binding.ObjectExpression;
import org.apache.avro.Schema;
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
import java.util.*;


public class AvroDataReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private static final Properties properties = PropertiesUtility.defaultProperties();
    private static final String avroPath = properties.getProperty("AVRO_HDFS_PATH");
    private String dataBase;
    private String tableName;
    private String recordId;
    private String recordLastUpdateTime;

    @Override
    public Map<String, Map<String, Long>> readSrcData(String filePath) {
        InputStream is;
        Map<String, Map<String, Long>> recordMap;
        FileSystem fs = HDFSFileUtility.getFileSystem(avroPath);
        try {
            if (null != fs) {
                is = fs.open(new Path(filePath));
                if (null != is) {
                    recordMap = readFromAvro(is);
                    return recordMap;
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
    private Map<String, Map<String, Long>> readFromAvro(InputStream is) {
        Map<String, Map<String, Long>> recordMap = new HashMap<>(2);
        if (null != recordId && null != recordLastUpdateTime) {
            Map<String, Long> createMap = new HashMap<>();
            Map<String, Long> updateMap = new HashMap<>();
            Map<String, Long> deleteMap = new HashMap<>();

            DataFileStream<Object> reader = null;
            try {
                reader = new DataFileStream<>(is, new GenericDatumReader<>());
            } catch (IOException e) {
                e.printStackTrace();
            }
            Iterator<Object> iterator = null;
            if (reader != null) {
                iterator = reader.iterator();
            }
            if (iterator != null) {
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
                    if (jsonObject != null) {
                        String id = String.valueOf(jsonObject.get(recordId));
                        long lastUpdateTime = jsonObject.getLong(recordLastUpdateTime);
                        switch (operator) {
                            case "Create":
                                createMap.put(id, lastUpdateTime);
                                break;
                            case "Update":
                                updateMap.put(id, lastUpdateTime);
                                break;
                            case "Delete":
                                deleteMap.put(id, lastUpdateTime);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            IOUtils.cleanup(null, is);
            IOUtils.cleanup(null, reader);
            createMap = BaseDataCompare.diffCompare(createMap, updateMap);
            if (createMap != null) {
                createMap = BaseDataCompare.diffCompare(createMap, deleteMap);
            }
            updateMap = BaseDataCompare.diffCompare(updateMap, deleteMap);
            recordMap.put(OperateType.Delete.toString(), deleteMap);
            recordMap.put(OperateType.Create.toString(), createMap);
            recordMap.put(OperateType.Update.toString(), updateMap);
            return recordMap;
        }
        return null;

    }

    public static Map<String, List<Set<Map.Entry<String, Object>>>> readAllDataFromAvro(String filePath) {
        InputStream is;
        Map<String, List<Set<Map.Entry<String, Object>>>> oprRecordMap = null;
        FileSystem fs = HDFSFileUtility.getFileSystem(avroPath);
        if (null != fs) {
            try {
                is = fs.open(new Path(filePath));
                DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
                Iterator<Object> iterator = reader.iterator();
                List<Set<Map.Entry<String, Object>>> createList = new ArrayList<>();
                List<Set<Map.Entry<String, Object>>> updateList = new ArrayList<>();
                List<Set<Map.Entry<String, Object>>> deleteList = new ArrayList<>();
                List<Set<Map.Entry<String, Object>>> upsertList = new ArrayList<>();
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
                    switch (operator) {
                        case "Create":
                            createList.add(jsonObject.entrySet());
                            break;
                        case "Update":
                            updateList.add(jsonObject.entrySet());
                            break;
                        case "Delete":
                            deleteList.add(jsonObject.entrySet());
                            break;
                        default:
                            break;
                    }
                }
                upsertList.addAll(createList);
                upsertList.addAll(updateList);
                oprRecordMap = new HashMap<>(3);
                oprRecordMap.put(OperateType.Unique.toString(), upsertList);
                oprRecordMap.put(OperateType.Delete.toString(), deleteList);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return oprRecordMap;
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
