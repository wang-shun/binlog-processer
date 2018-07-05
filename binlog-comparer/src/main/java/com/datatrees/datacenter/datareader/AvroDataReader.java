package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AvroDataReader extends DataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private final FileSystem fs = HDFSFileUtility.fileSystem;
    private final String RECORD_ID = "id";
    private final String RECORD_LAST_UPDATE_TIME = "LastUpdatedDatetime";

    @Override
    public Map<String, Map<String, Long>> readSrcData(String filePath) {
        List<String> fileList = HDFSFileUtility.getFilesPath(filePath);
        Map<String, Map<String, Long>> operateMap = new HashMap<>();
        Map<String, Long> createMap = new HashMap<>();
        Map<String, Long> upDateMap = new HashMap<>();
        Map<String, Long> deleteMap = new HashMap<>();
        if (fileList.size() > 0) {
            InputStream is;
            for (String file : fileList) {
                try {
                    is = fs.open(new Path(file));
                    Map<String, Map<String, Long>> recordMap = readFromAvro(is);
                    createMap.putAll(recordMap.get(OperateType.Create.toString()));
                    upDateMap.putAll(recordMap.get(OperateType.Update.toString()));
                    deleteMap.putAll(recordMap.get(OperateType.Delete.toString()));
                } catch (IOException e) {
                    LOG.error("can't open HDFS file :" + filePath);
                }
                operateMap.put(OperateType.Create.toString(), createMap);
                operateMap.put(OperateType.Update.toString(), upDateMap);
                operateMap.put(OperateType.Delete.toString(), deleteMap);
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
    private Map<String, Map<String, Long>> readFromAvro(InputStream is) {
        Map<String, Long> createMap = new HashMap<>();
        Map<String, Long> upDateMap = new HashMap<>();
        Map<String, Long> deleteMap = new HashMap<>();
        Map<String, Map<String, Long>> recordMap = new HashMap<>(3);
        try {
            DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
            for (Object o : reader) {
                GenericRecord r = (GenericRecord) o;
                JSONObject jsonObject = JSONObject.parseObject(r.get(1).toString());
                String id = String.valueOf(jsonObject.get(RECORD_ID));
                long lastUpdateTime = jsonObject.getLong(RECORD_LAST_UPDATE_TIME);

                String operator = r.get(2).toString();
                switch (operator) {
                    case "Create":
                        createMap.put(id, lastUpdateTime);
                        break;
                    case "Update":
                        upDateMap.put(id, lastUpdateTime);
                        break;
                    case "Delete":
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
            recordMap.put(OperateType.Unique.toString(),allDataMap);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return recordMap;
    }
}
