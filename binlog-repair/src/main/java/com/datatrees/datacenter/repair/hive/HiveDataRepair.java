package com.datatrees.datacenter.repair.hive;


import com.datatrees.datacenter.core.utility.IpMatchUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.partitions.partitionHandler;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.repair.transaction.TransactionOperate;
import com.datatrees.datacenter.table.CheckResult;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;


public class HiveDataRepair implements BaseDataRepair {
    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);
    private String hiveCheckByFileTable = "t_binlog_check_hive";

    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {

    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {

        String dataBase = checkResult.getDataBase();
        String dbInstance = checkResult.getDbInstance();
        String tableName = checkResult.getTableName();
        String partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        String fileName = checkResult.getFileName();

        String hivePartition;
        List<String> hivePartitions;
        if (partition != null) {
            Map<String, String> dateMap = partitionHandler.getDateMap(partition);
            hivePartition = partitionHandler.getHivePartition(dateMap);
            hivePartitions = partitionHandler.getPartitions(dateMap);

            String filePath = FileOperate.getFilePath(dataBase, dbInstance, tableName, partition, partitionType, fileName);
            Map<String, List<String>> opIdMap = BinlogDBHandler.getOpreateIdList(checkResult, hiveCheckByFileTable);
            if (opIdMap != null && opIdMap.size() > 0) {
                InputStream inputStream = FileOperate.getHdfsFileInputStream(filePath);
                if (inputStream != null) {
                    Iterator iterator = opIdMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        String operate = String.valueOf(entry.getKey());
                        List<String> idList = (List<String>) entry.getValue();
                        if (idList != null && idList.size() > 0) {
                            Map<String, Object> genericRecordListMap = AvroDataBuilder.avroSchemaDataBuilder(inputStream, idList, operate);
                            if (genericRecordListMap != null) {
                                Schema schema = (Schema) genericRecordListMap.get("schema");
                                List<GenericData.Record> genericRecordList = (List<GenericData.Record>) genericRecordListMap.get("record");
                                TransactionOperate.repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, genericRecordList);
                                BinlogDBHandler.updateCheckedFile(checkTable, dataBase, tableName, partition, operate, partitionType);
                            } else {
                                LOG.info("no data record read from the avro file");
                            }
                        } else {
                            LOG.info("no id find of operate :" + operate);
                        }
                    }
                } else {
                    LOG.info("can't get the avro file inputStream from HDFS");
                }
            } else {
                LOG.info("no id list find from the database");
            }
        }
    }
}
