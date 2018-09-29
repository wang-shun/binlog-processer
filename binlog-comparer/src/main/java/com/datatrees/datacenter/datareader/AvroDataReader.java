package com.datatrees.datacenter.datareader;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


public class AvroDataReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataReader.class);
    private static final Properties PROPERTIES = PropertiesUtility.defaultProperties();
    private static final String AVRO_PATH = PROPERTIES.getProperty("AVRO_HDFS_PATH");
    private static final List<String> ID_COLUMN_LIST = FieldNameOp.getConfigField("id");
    private static final List<String> LAST_UPDATE_COLUMN_LIST = FieldNameOp.getConfigField("update");
    private String dataBase;
    private String tableName;
    private String recordId;
    private String recordLastUpdateTime;

    @Override
    public Map<String, Map<String, Long>> readSrcData(String filePath) {
        InputStream is;
        Map<String, Map<String, Long>> recordMap;
        FileSystem fs = HDFSFileUtility.getFileSystem(AVRO_PATH);
        try {
            if (null != fs) {
                is = fs.open(new Path(filePath));
                if (null != is) {
                    recordMap = readFromAvro(is);
                    return recordMap;
                }
            }
        } catch (IOException e) {
            LOG.info("File :" + filePath + " doesn't exist");
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
        Map<String, Map<String, Long>> recordMap = new HashMap<>(3);
        Map<String, Long> createMap = new HashMap<>();
        Map<String, Long> updateMap = new HashMap<>();
        Map<String, Long> deleteMap = new HashMap<>();

        DataFileStream<Object> reader = null;
        try {
            reader = new DataFileStream<>(is, new GenericDatumReader<>());
            Iterator<Object> iterator = reader.iterator();
            List<Schema.Field> fieldList = reader.getSchema().getField("After").schema().getTypes().get(1).getFields();
            Set<String> fieldSet = new HashSet<>(fieldList.size());
            fieldList.forEach(x -> fieldSet.add(x.name()));
            recordId = FieldNameOp.getFieldName(fieldSet, ID_COLUMN_LIST);
            LOG.info("the id field name is :" + recordId);
            recordLastUpdateTime = FieldNameOp.getFieldName(fieldSet, LAST_UPDATE_COLUMN_LIST);
            LOG.info("the lastUpdateTime field is :" + recordLastUpdateTime);

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
                    String lastUpdateTime = String.valueOf(jsonObject.getLong(recordLastUpdateTime));

                    if (id != null && lastUpdateTime != null) {
                        if (!"null".equals(id) && !"null".equals(lastUpdateTime)) {
                            long timeStamp = Long.parseLong(lastUpdateTime);
                            switch (operator) {
                                case "Create":
                                    createMap.put(id, timeStamp);
                                    break;
                                case "Update":
                                    updateMap.put(id, timeStamp);
                                    break;
                                case "Delete":
                                    deleteMap.put(id, timeStamp);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }
            createMap = BaseDataCompare.diffCompare(createMap, updateMap);
            if (createMap != null) {
                createMap = BaseDataCompare.diffCompare(createMap, deleteMap);
            }
            updateMap = BaseDataCompare.diffCompare(updateMap, deleteMap);
            recordMap.put(OperateType.Delete.toString(), deleteMap);
            recordMap.put(OperateType.Create.toString(), createMap);
            recordMap.put(OperateType.Update.toString(), updateMap);
            return recordMap;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 读取某个分区目录下的所有文件
     * @param filePath 文件路径
     * @return Avro记录
     */
    public static Map<String, List<Set<Map.Entry<String, Object>>>> readAllDataFromAvro(String filePath) {
        InputStream is;
        Map<String, List<Set<Map.Entry<String, Object>>>> oprRecordMap = null;
        FileSystem fs = HDFSFileUtility.getFileSystem(AVRO_PATH);
        if (null != fs) {
            try {
                is = fs.open(new Path(filePath));
                DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
                Iterator<Object> iterator = reader.iterator();
                List<Set<Map.Entry<String, Object>>> createList = new ArrayList<>();
                List<Set<Map.Entry<String, Object>>> updateList = new ArrayList<>();
                List<Set<Map.Entry<String, Object>>> deleteList = new ArrayList<>();
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
                is.close();
                reader.close();
                oprRecordMap = new HashMap<>(3);
                oprRecordMap.put(OperateType.Create.toString(), createList);
                oprRecordMap.put(OperateType.Update.toString(), updateList);
                oprRecordMap.put(OperateType.Delete.toString(), deleteList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return oprRecordMap;
    }

    /**
     * 读出指定avro中指定id的数据
     *
     * @param checkResult 表信息
     * @param checkTable  检查信息表
     * @return Avro记录
     */
    public static Map<String, List<Set<Map.Entry<String, Object>>>> readAvroDataById(CheckResult checkResult, String checkTable) {
        Map<String, Object> whereMap = new HashMap<>();
        String dbInstance = checkResult.getDbInstance();
        String dataBase = checkResult.getDataBase();
        String tableName = checkResult.getTableName();
        String partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        whereMap.put(CheckTable.DB_INSTANCE, dbInstance);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.values().remove("");
        StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
        String sql = "select id_list,files_path,operate_type from " + checkTable + " " + whereExpress;
        Map<String, List<Set<Map.Entry<String, Object>>>> dataRecord = null;
        try {
            List<Map<String, Object>> missingData = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, sql);
            if (missingData != null && missingData.size() > 0) {
                Map<String, List<String>> opIdMap = new HashMap<>(missingData.size());
                String filesPath = null;
                for (Map<String, Object> record : missingData) {
                    String[] idArr = record.get(CheckTable.ID_LIST).toString().replace("[", "").replace("]", "").split(",");
                    List<String> idList = Arrays.asList(idArr);
                    List<String> idListNew = idList.stream().map(String::trim).collect(Collectors.toList());
                    filesPath = (String) record.get(CheckTable.FILES_PATH);
                    String operateType = (String) record.get(CheckTable.OP_TYPE);
                    opIdMap.put(operateType, idListNew);
                }
                dataRecord = filterDataByIdList(filesPath, dataBase, tableName, opIdMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataRecord;
    }

    /**
     * 根据ID过滤数据
     * @param filePath 文件路径
     * @param dataBase 数据库
     * @param tableName 表
     * @param opIdMap 操作类型map
     * @return Avro记录
     */
    private static Map<String, List<Set<Map.Entry<String, Object>>>> filterDataByIdList(String filePath, String
            dataBase, String tableName, Map<String, List<String>> opIdMap) {
        InputStream is;
        List<String> fileList = HDFSFileUtility.getFilesPath(filePath);
        Collections.sort(fileList);
        Map<String, List<Set<Map.Entry<String, Object>>>> oprRecordMap = new HashMap<>(opIdMap.size());
        for (String key : opIdMap.keySet()) {
            List<Set<Map.Entry<String, Object>>> recordList = new ArrayList<>();
            oprRecordMap.put(key, recordList);
        }
        FileSystem fs = HDFSFileUtility.getFileSystem(AVRO_PATH);
        if (null != fs) {
            try {
                Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
                String recordId = FieldNameOp.getFieldName(allFieldSet, ID_COLUMN_LIST);
                for (String aFileList : fileList) {
                    Path path = new Path(aFileList);
                    is = fs.open(path);
                    DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());
                    Iterator<Object> iterator = reader.iterator();
                    while (iterator.hasNext()) {
                        Object o = iterator.next();
                        GenericRecord r = (GenericRecord) o;
                        JSONObject jsonObject;
                        if (null != r.get(1)) {
                            jsonObject = JSONObject.parseObject(r.get(1).toString());
                        } else {
                            jsonObject = JSONObject.parseObject(r.get(0).toString());
                        }
                        String id = String.valueOf(jsonObject.get(recordId));
                        for (Map.Entry operateRecord : opIdMap.entrySet()) {
                            String operateType = String.valueOf(operateRecord.getKey());
                            List<String> idList = (List<String>) operateRecord.getValue();
                            if (idList.contains(id)) {
                                String operator = r.get(2).toString();
                                if (operateType.equals(operator)) {
                                    oprRecordMap.get(operateType).add(jsonObject.entrySet());
                                }
                            }
                        }
                    }
                }
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
