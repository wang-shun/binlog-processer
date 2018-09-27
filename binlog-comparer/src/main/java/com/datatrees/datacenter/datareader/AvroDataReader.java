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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
    private static final List<String> lastUpdateColumnList = FieldNameOp.getConfigField("update");
    private static final String AFTER_TAG = "After";
    private static final String OP_TAG = "op";
    private static final String KEY_TAG = "key";
    private static final String HIVE_AFTER_TAG = "after";
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
                    avroSchemaConvert(is);
                    return null;
                    /*recordMap = readFromAvro(is);
                    return recordMap;*/
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
            recordLastUpdateTime = FieldNameOp.getFieldName(fieldSet, lastUpdateColumnList);
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
                if (jsonObject!= null) {
                    String id = String.valueOf(jsonObject.get(recordId));
                    String lastUpdateTime = String.valueOf(jsonObject.getLong(recordLastUpdateTime));

                    if (id != null && lastUpdateTime != null) {
                        if (!"null".equals(id)&&!"null".equals(lastUpdateTime)) {
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


    private static List<GenericRecord> avroSchemaConvert(InputStream inputStream) {
        DataFileStream<Object> reader;
        try {
            reader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
            Schema schema = reader.getSchema();
            String name = schema.getName();
            String nameSpace = schema.getNamespace();

            Schema afterSchema = reader.getSchema().getField(AFTER_TAG).schema().getTypes().get(1);
            List<Schema.Field> fields = afterSchema.getFields();
            Set<String> fieldName = new HashSet<>(fields.size());
            fields.forEach(x -> fieldName.add(x.schema().getName()));
            String idField = FieldNameOp.getFieldName(fieldName, ID_COLUMN_LIST);
            if (idField != null) {
                for (Schema.Field field : fields) {
                    System.out.println(field);
                    System.out.println(field.schema().getType());
                }
                Schema opSchema = reader.getSchema().getField(OP_TAG).schema();
                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                        .record(name)
                        .namespace(nameSpace).fields();

                fieldAssembler.name(HIVE_AFTER_TAG).type(afterSchema).noDefault();
                fieldAssembler.name(OP_TAG).type(opSchema).noDefault();
                fieldAssembler.name(KEY_TAG).type(SchemaBuilder
                        .record(KEY_TAG)
                        .namespace(name)
                        .fields()
                        .name("Key")
                        .type(Schema.create(Schema.Type.LONG)).noDefault().endRecord()).noDefault();

                Schema finalcSchema = fieldAssembler.endRecord();

                System.out.println(finalcSchema);
                Iterator iterator = reader.iterator();
                GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(finalcSchema);
                List<GenericRecord> genericRecordList = new ArrayList<>();
                while (iterator.hasNext()) {
                    GenericRecord genericRecord = (GenericRecord) iterator.next();
                    genericRecordBuilder.set(HIVE_AFTER_TAG, genericRecord.get(1));
                    genericRecordBuilder.set(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                    genericRecordBuilder.set(KEY_TAG, idField);
                    GenericData.Record record = genericRecordBuilder.build();
                    genericRecordList.add(record);
                    System.out.println(record.toString());
                }
                return genericRecordList;
            }
        } catch (IOException e) {
            LOG.info("can't not read data from avro file with error info :", e);
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
