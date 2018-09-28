package com.datatrees.datacenter.repair.hive;


import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.repair.schema.SchemaConvertor;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;


public class HiveDataRepair implements BaseDataRepair {
    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String AVRO_HDFS_PATH = properties.getProperty("AVRO_HDFS_PATH");
    private static final String metastoreUris = "thrift://cdh2:9083";
    private static final String FILE_SEP = File.separator;
    private static final String DATE_SEP = "=";
    private String hiveCheckTableByFile = "t_binlog_check_hive";


    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {

    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {
        // TODO: 2018/9/28 先从数据库查询需要修复的数据信息 ，在读Avro，再调用接口
        HiveConf hiveConf = new HiveConf();
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Schema schema = null;

        String dataBase = checkResult.getDataBase();
        String dbInstance = checkResult.getDbInstance();
        String tableName = checkResult.getTableName();
        String partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        String fileName = checkResult.getFileName();
        String filePath;
        StringBuilder filePathBuilder = new StringBuilder();

        String hivePartition;
        List<String> hivePartitions;
        if (partition != null) {
            String[] date = partition.split(FILE_SEP);
            String year = date[0].split(DATE_SEP)[1];
            String month = date[1].split(DATE_SEP)[1];
            String day = date[2].split(DATE_SEP)[1];

            StringBuilder hivePartitionBuilder = new StringBuilder();
            hivePartitionBuilder.append("p_y=")
                    .append(year)
                    .append(File.separator)
                    .append("p_m")
                    .append(month)
                    .append(File.separator)
                    .append("p_d")
                    .append(day);

            hivePartition = hivePartitionBuilder.toString();
            hivePartitions = Arrays.asList(date);

            // TODO: 2018/9/28 idc和阿里云使用了相同的路径
            if (dbInstance != null || !"null".equals(dbInstance)) {
                filePathBuilder
                        .append(AVRO_HDFS_PATH)
                        .append(File.separator)
                        .append(partitionType)
                        .append(File.separator)
                        .append(dbInstance)
                        .append(File.separator)
                        .append(dataBase)
                        .append(File.separator)
                        .append(tableName)
                        .append(File.separator)
                        .append(partition)
                        .append(File.separator)
                        .append(fileName);
            } else {
                filePathBuilder
                        .append(AVRO_HDFS_PATH)
                        .append(File.separator)
                        .append(partitionType)
                        .append(File.separator)
                        .append(dataBase)
                        .append(File.separator)
                        .append(tableName)
                        .append(File.separator)
                        .append(partition)
                        .append(File.separator)
                        .append(fileName);
            }
            filePath = filePathBuilder.toString();
            InputStream inputStream = FileOperate.getHdfsFileInput(filePath);
            Map<String, List<String>> opIdMap = BinlogDBHandler.readAvroDataById(checkResult, hiveCheckTableByFile);
            if (inputStream != null) {
                if (opIdMap != null && opIdMap.size() > 0) {
                    Iterator iterator = opIdMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        String operate = String.valueOf(entry.getKey());
                        List<String> idList = (List<String>) entry.getValue();
                        List<GenericRecord> genericRecordList = AvroDataBuilder.avroSchemaDataBuilder(inputStream, idList, operate);
                        if (OperateType.Create.toString().equals(operate)) {
                            InsertMutation mutation = new InsertMutation(dataBase, tableName, hivePartition, hivePartitions, metastoreUris, hBaseConfiguration);
                            for (int i = 0; i < genericRecordList.size(); i++) {
                                GenericData.Record record = (GenericData.Record) genericRecordList.get(i);
                                try {
                                    mutation.beginFixTransaction(schema, hiveConf);
                                    mutation.insert(record);
                                    mutation.commitTransaction();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        } else {
                            UpdateMutation mutation = new UpdateMutation(dataBase, tableName, hivePartition, hivePartitions, metastoreUris, hBaseConfiguration);
                            for (GenericRecord record : genericRecordList) {
                                try {
                                    mutation.beginFixTransaction(schema, hiveConf);
                                    mutation.update((GenericData.Record) record, false);
                                    mutation.commitTransaction();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        Map<String, Object> whereMap = new HashMap<>();
                        whereMap.put(CheckTable.DB_INSTANCE, dbInstance);
                        whereMap.put(CheckTable.DATA_BASE, dataBase);
                        whereMap.put(CheckTable.TABLE_NAME, tableName);
                        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
                        whereMap.put(CheckTable.FILE_PARTITION, partition);
                        whereMap.values().remove("");
                        StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
                        String sql = "select id_list,files_path,operate_type from " + checkTable + " " + whereExpress;
                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put(CheckTable.STATUS, 1);
                        try {
                            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, checkTable, valueMap, whereMap);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
