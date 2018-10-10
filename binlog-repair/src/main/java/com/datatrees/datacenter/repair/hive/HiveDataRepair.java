package com.datatrees.datacenter.repair.hive;


import com.datatrees.datacenter.core.threadpool.ThreadPoolInstance;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.partitions.partitionHandler;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.repair.transaction.TransactionOperate;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * @author personalc
 */
public class HiveDataRepair implements BaseDataRepair {

    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);

    private Properties properties = PropertiesUtility.defaultProperties();

    private String binlogDataBase = properties.getProperty("jdbc.database");

    private String dbInstance;

    private String tableName;

    private String dataBase;

    private String partition;

    private ThreadPoolExecutor executors;


    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.FILE_NAME, fileName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        try {
            String binlogProcessLogTable = "t_binlog_process_log";
            List<Map<String, Object>> fileInfos = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), binlogDataBase, binlogProcessLogTable, whereMap);
            if (fileInfos != null && fileInfos.size() > 0) {
                executors = ThreadPoolInstance.getExecutors();
                for (Map<String, Object> fileInfo : fileInfos) {
                    dbInstance = String.valueOf(fileInfo.get(CheckTable.DB_INSTANCE)).replaceAll("_", ".");
                    dataBase = String.valueOf(fileInfo.get(CheckTable.DATA_BASE));
                    tableName = String.valueOf(fileInfo.get(CheckTable.TABLE_NAME));
                    partition = String.valueOf(fileInfo.get(CheckTable.FILE_PARTITION));
                    String filePath = FileOperate.getFilePath(dataBase, dbInstance, tableName, partition, partitionType, fileName);
                    System.out.println(filePath);
                    RepairThread repairThread = new RepairThread(tableName, dataBase, partition, filePath, fileName, partitionType);
                    executors.submit(repairThread);
                }
                executors.shutdown();
                while (true) {
                    if (executors.isTerminated()) {
                        System.out.println("all threads has finished！");
                        BinlogDBHandler.updateBinlogProcessLog(binlogProcessLogTable, fileName, dataBase, tableName, partition, partitionType);
                        break;
                    }
                    Thread.sleep(1000);
                }

            } else {
                LOG.info("no avro files find in the database");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {

        dataBase = checkResult.getDataBase();
        dbInstance = checkResult.getDbInstance();
        tableName = checkResult.getTableName();
        partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        String fileName = checkResult.getFileName();

        if (partition != null) {
            Map<String, String> dateMap = partitionHandler.getDateMap(partition);
            String hivePartition = partitionHandler.getHivePartition(dateMap);
            List<String> hivePartitions = partitionHandler.getPartitions(dateMap);

            String filePath = FileOperate.getFilePath(dataBase, dbInstance, tableName, partition, partitionType, fileName);
            Map<String, List<String>> opIdMap = BinlogDBHandler.getOpreateIdList(checkResult, checkTable);

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
                                String schemaName = "schema";
                                Schema schema = (Schema) genericRecordListMap.get(schemaName);
                                String recordName = "record";
                                List<GenericData.Record> genericRecordList = (List<GenericData.Record>) genericRecordListMap.get(recordName);
                                TransactionOperate.repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, genericRecordList);
                                LOG.info("operate type:[" + operate + "] ," + "record number:[" + genericRecordList.size() + "]");
                                BinlogDBHandler.updateCheckedFile(checkTable, fileName, dataBase, tableName, partition, operate, partitionType);
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
