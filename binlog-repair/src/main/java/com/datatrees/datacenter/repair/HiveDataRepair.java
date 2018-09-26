package com.datatrees.datacenter.repair;


import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.com.esotericsoftware.minlog.Log;

import java.sql.SQLException;
import java.util.*;


public class HiveDataRepair implements BaseDataRepair {

    private static final String metastoreUris = "thrift://cdh2:9083";


    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {

    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {
        Map<String, List<Set<Map.Entry<String, Object>>>> dataMap = AvroDataReader.readAvroDataById(checkResult, checkTable);
        if (dataMap != null && dataMap.size() > 0) {
            String dataBase = checkResult.getDataBase();
            String table = checkResult.getTableName();
            for (Map.Entry dataEntry : dataMap.entrySet()) {
                if (OperateType.Create.equals(dataEntry.getKey())) {

                } else {
                   /* InsertMutation mutation = new InsertMutation(dataBase, table, "", new ArrayList<>(), metastoreUris, new HBaseConfiguration());
                    mutation.beginFixTransaction();
                    mutation.insert();
                    try {
                        mutation.commitTransaction();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/
                }
            }
            Map<String, Object> whereMap = new HashMap<>();
            String dbInstance = checkResult.getDbInstance();
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
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put(CheckTable.STATUS, 1);
            try {
                DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, checkTable, valueMap, whereMap);
            } catch (SQLException e) {
                Log.error("update repaired record in " + checkTable + " failed with Exception :", e);
            }
        }
    }
}
