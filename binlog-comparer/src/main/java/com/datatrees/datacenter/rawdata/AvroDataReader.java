package com.datatrees.datacenter.rawdata;

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

public class AvroDataReader implements DataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private final FileSystem fs = HDFSFileUtility.fileSystem;

    @Override
    public Map<String, List> readAllData(String path) {
        InputStream is;
        List<String> fileList = getFilesPath(path);
        Map<String, List> opRecord = new HashMap<>();
        if (fileList.size() > 0) {
            List<Integer> createList = new ArrayList<>();
            List<Map<Integer, Long>> upDateList = new ArrayList<>();
            List<Integer> deleteList = new ArrayList<>();
            for (String filePath : fileList) {
                try {
                    is = fs.open(new Path(filePath));
                    Map<String, List> recordMap = readFromAvro(is);
                    createList.addAll(recordMap.get(OperateType.Create.toString()));
                    upDateList.addAll(recordMap.get(OperateType.Update.toString()));
                    deleteList.addAll(recordMap.get(OperateType.Delete.toString()));
                } catch (IOException e) {
                    LOG.error("can't open HDFS file :" + filePath);
                }
            }
            opRecord.put(OperateType.Create.toString(), createList);
            opRecord.put(OperateType.Update.toString(), upDateList);
            opRecord.put(OperateType.Delete.toString(), deleteList);
        }
        return opRecord;
    }

    @Override
    public List<String> getFilesPath(String destPath) {
        List<String> fileList;
        fileList = HDFSFileUtility.getFilesPath(destPath);
        return fileList;
    }

    /**
     * 读取Avro文件中的记录并根据事件分类
     *
     * @param is 输入文件流
     * @return Avro中分类后的事件信息
     */
    private Map<String, List> readFromAvro(InputStream is) {
        List<Integer> createList = new ArrayList<>();
        List<Map<Integer, Long>> upDateList = new ArrayList<>();
        List<Integer> deleteList = new ArrayList<>();
        Map<String, List> opRecord = new HashMap<>(3);
        try {
            DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
            for (Object o : reader) {
                GenericRecord r = (GenericRecord) o;

                JSONObject jsonObject = JSONObject.parseObject(r.get(1).toString());
                int id = (Integer) jsonObject.get("Id");
                long lastUpdateTime = jsonObject.getLong("LastUpdatedDatetime");

                String operator = r.get(2).toString();
                switch (operator) {
                    case "Create":
                        createList.add(id);
                        break;
                    case "Update":
                        Map<Integer, Long> idUpdateTime = new HashMap<>(1);
                        idUpdateTime.put(id, lastUpdateTime);
                        upDateList.add(idUpdateTime);
                        break;
                    case "Delete":
                        deleteList.add(id);
                        break;
                    default:
                        break;
                }
            }
            opRecord.put(OperateType.Create.toString(), createList);
            opRecord.put(OperateType.Update.toString(), upDateList);
            opRecord.put(OperateType.Delete.toString(), deleteList);
            IOUtils.cleanup(null, is);
            IOUtils.cleanup(null, reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return opRecord;
    }
}
