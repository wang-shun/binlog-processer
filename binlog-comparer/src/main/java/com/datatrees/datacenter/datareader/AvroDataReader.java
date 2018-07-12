package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.FieldNameOp;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class AvroDataReader extends DataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");
    private String dataBase;
    private String tableName;

    @Override
    public Map<String, Map<String, Long>> readSrcData(String filePath) {
        List<String> fileList = HDFSFileUtility.getFilesPath(filePath);
        Map<String, Map<String, Long>> operateMap = new HashMap<>();
        Map<String, Long> createMap = new HashMap<>();
        Map<String, Long> upDateMap = new HashMap<>();
        Map<String, Long> deleteMap = new HashMap<>();
        Map<String, Long> uniqueMap = new HashMap<>();
        // TODO: 2018/7/11 同一张表的数据可以一次读取
        if (fileList.size() > 0) {
            InputStream is;
            for (String file : fileList) {
                try {
                    is = HDFSFileUtility.getFileSystem(file).open(new Path(file));
                    Map<String, Map<String, Long>> recordMap = readFromAvro(dataBase, tableName, is);
                    createMap.putAll(recordMap.get(OperateType.Create.toString()));
                    upDateMap.putAll(recordMap.get(OperateType.Update.toString()));
                    deleteMap.putAll(recordMap.get(OperateType.Delete.toString()));
                    uniqueMap.putAll(recordMap.get(OperateType.Unique.toString()));
                } catch (IOException e) {
                    LOG.error("can't open HDFS file :" + filePath);
                }
                operateMap.put(OperateType.Create.toString(), createMap);
                operateMap.put(OperateType.Update.toString(), upDateMap);
                operateMap.put(OperateType.Delete.toString(), deleteMap);
                operateMap.put(OperateType.Unique.toString(),uniqueMap);
            }
        }
        return operateMap;
    }

    /**
     * 读取Avro文件中的记录并根据事件分类
     *
     * @param is 输入文件流
     * @return Avro中分类后的事件信息
     */
    private Map<String, Map<String, Long>> readFromAvro(String dataBase, String tableName, InputStream is) {
        // TODO: 2018/7/11
        String RECORD_ID = FieldNameOp.getFieldName(dataBase, tableName, idList);
        String RECORD_LAST_UPDATE_TIME = FieldNameOp.getFieldName(dataBase, tableName, createTimeList);

        Map<String, Long> createMap = new HashMap<>();
        Map<String, Long> upDateMap = new HashMap<>();
        Map<String, Long> deleteMap = new HashMap<>();
        Map<String, Map<String, Long>> recordMap = new HashMap<>(3);
        try {
            DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
            for (Object o : reader) {
                GenericRecord r = (GenericRecord) o;
                String operator = r.get(2).toString();
                JSONObject jsonObject;
                String id;
                long lastUpdateTime;
                switch (operator) {
                    case "Create":
                        jsonObject = JSONObject.parseObject(r.get(1).toString());
                        id = String.valueOf(jsonObject.get(RECORD_ID));
                        lastUpdateTime = jsonObject.getLong(RECORD_LAST_UPDATE_TIME);
                        createMap.put(id, lastUpdateTime);
                        break;
                    case "Update":
                        jsonObject = JSONObject.parseObject(r.get(1).toString());
                        id = String.valueOf(jsonObject.get(RECORD_ID));
                        lastUpdateTime = jsonObject.getLong(RECORD_LAST_UPDATE_TIME);
                        upDateMap.put(id, lastUpdateTime);
                        break;
                    case "Delete":
                        jsonObject = JSONObject.parseObject(r.get(0).toString());
                        id = String.valueOf(jsonObject.get(RECORD_ID));
                        lastUpdateTime = jsonObject.getLong(RECORD_LAST_UPDATE_TIME);
                        deleteMap.put(id, lastUpdateTime);
                        break;
                    default:
                        break;
                }
            }
            IOUtils.cleanup(null, is);
            IOUtils.cleanup(null, reader);

            recordMap.put(OperateType.Create.toString(), createMap);
            recordMap.put(OperateType.Update.toString(), upDateMap);
            recordMap.put(OperateType.Delete.toString(), deleteMap);
            List<Map<String, Long>> mapList = new ArrayList<>();
            mapList.add(createMap);
            mapList.add(upDateMap);
            mapList.add(deleteMap);
            Map<String, Long> allDataMap = new HashMap<>();
            mapList.forEach(allDataMap::putAll);
            recordMap.put(OperateType.Unique.toString(), allDataMap);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return recordMap;
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
}
