package com.datatrees.datacenter.rawdata;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroDataReader implements DataReader {
    private final FileSystem fs = HDFSFileUtility.fileSystem;
    private final String operatorField = "op";

    @Override
    public void readBinLogData(String Path) {
        InputStream is;
        try {
            List<String> fileList = getFilesPath(Path);
            if (fileList.size() > 0) {
                for (String filePath : fileList) {
                    is = fs.open(new Path(filePath));
                    readFromAvro(is);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public List<String> getFilesPath(String destPath) {
        List<String> fileList;
        fileList = HDFSFileUtility.getFilesPath(destPath);
        return fileList;
    }

    private void readFromAvro(InputStream is) {
        List<Integer> insertList = new ArrayList<>();
        List<Map<Integer, Long>> upDateList = new ArrayList<>();
        List<Integer> deleteList = new ArrayList<>();
        try {
            DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
            for (Object o : reader) {
                GenericRecord r = (GenericRecord) o;

                JSONObject jsonObject = JSONObject.parseObject(r.get(1).toString());
                int id = (Integer) jsonObject.get("Id");
               // System.out.println("id===========" + id);

                long lastUpdateTime = jsonObject.getLong("LastUpdatedDatetime");
                System.out.println(lastUpdateTime);

                String operator = r.get(2).toString();
                System.out.println(r.get(2).toString());
                switch (operator) {
                    case "Create":
                        insertList.add(id);
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
            IOUtils.cleanup(null, is);
            IOUtils.cleanup(null, reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
