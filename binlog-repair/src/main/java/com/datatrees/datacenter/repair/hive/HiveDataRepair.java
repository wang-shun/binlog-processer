package com.datatrees.datacenter.repair.hive;


import com.datatrees.datacenter.core.utility.IpMatchUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.table.CheckResult;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;


public class HiveDataRepair implements BaseDataRepair {
    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String AVRO_HDFS_PATH = properties.getProperty("AVRO_HDFS_PATH");
    private static final String metastoreUris = "thrift://hadoop3:9083";
    private static final String FILE_SEP = File.separator;
    private static final String DATE_SEP = "=";
    private String hiveCheckTableByFile = "t_binlog_check_hive";
    private HiveConf hiveConf;
    private HBaseConfiguration hBaseConfiguration;


    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {

    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {
        hiveConf = new HiveConf();
        hBaseConfiguration = new HBaseConfiguration();

        String dataBase = checkResult.getDataBase();
        String dbInstance = checkResult.getDbInstance();
        String tableName = checkResult.getTableName();
        String partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        String fileName = checkResult.getFileName();

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
                    .append("p_m=")
                    .append(month)
                    .append(File.separator)
                    .append("p_d=")
                    .append(day);

            hivePartition = hivePartitionBuilder.toString();
            hivePartitions = new ArrayList<>();
            hivePartitions.add(year);
            hivePartitions.add(month);
            hivePartitions.add(day);

            // TODO: 2018/9/28 idc和阿里云使用了相同的路径
            StringBuilder filePathBuilder = new StringBuilder();
            if (IpMatchUtility.isboolIp(dbInstance)) {
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
                        .append(fileName)
                        .append(".avro");
            } else {
                filePathBuilder
                        .append(AVRO_HDFS_PATH.split("_")[0])
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
                        .append(fileName)
                        .append(".avro");
            }
            String filePath = filePathBuilder.toString();
            InputStream inputStream = FileOperate.getHdfsFileInput(filePath);
            Map<String, List<String>> opIdMap = BinlogDBHandler.readAvroDataById(checkResult, hiveCheckTableByFile);
            if (inputStream != null) {
                if (opIdMap != null && opIdMap.size() > 0) {
                    Iterator iterator = opIdMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        String operate = String.valueOf(entry.getKey());
                        List<String> idList = (List<String>) entry.getValue();
                        if (idList != null && idList.size() > 0) {
                            Map<String, Object> genericRecordListMap = AvroDataBuilder.avroSchemaDataBuilder(inputStream, idList, operate);
                            Schema schema = (Schema) genericRecordListMap.get("schema");
                            List<GenericData.Record> genericRecordList = (List<GenericData.Record>) genericRecordListMap.get("record");
                            repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, genericRecordList);
                            BinlogDBHandler.updateCheckedFile(checkTable, dataBase, dbInstance, tableName, partition, partitionType);
                        }
                    }
                }
            }
        }
    }

    private void repairTransaction(String dataBase, String tableName, String hivePartition, List<String> hivePartitions, String operate, Schema schema, List<GenericData.Record> genericRecordList) {
        if (OperateType.Create.toString().equals(operate)) {
            InsertMutation mutation = new InsertMutation(dataBase, tableName, hivePartition, hivePartitions, metastoreUris, hBaseConfiguration);
            try {
                mutation.beginFixTransaction(schema, hiveConf);
                for (GenericData.Record record : genericRecordList) {
                    mutation.insert(record);
                }
                mutation.commitTransaction();
            } catch (Exception e) {
                mutation.abortTxn();
                e.printStackTrace();
            }

        } else {
            UpdateMutation mutation = new UpdateMutation(dataBase, tableName, hivePartition, hivePartitions, metastoreUris, hBaseConfiguration);

            try {
                mutation.beginFixTransaction(schema, hiveConf);
                for (GenericData.Record record : genericRecordList) {

                    mutation.update(record);
                }
                mutation.commitTransaction();
            } catch (Exception e) {
                mutation.abortTxn();
                e.printStackTrace();
            }

        }
    }
}
