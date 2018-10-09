package com.datatrees.datacenter.repair.transaction;

import com.datatrees.datacenter.operate.OperateType;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.List;

public class TransactionOperate {
    private static HiveConf hiveConf = new HiveConf();
    private static Configuration configuration = HBaseConfiguration.create();
    private static final String METASTORE_URIS = "thrift://hadoop3:9083";

    public static void repairTransaction(String dataBase, String tableName, String hivePartition, List<String> hivePartitions, String operate, Schema schema, List<GenericData.Record> genericRecordList) {
        if (OperateType.Create.toString().equals(operate)) {
            InsertMutation mutation = new InsertMutation(dataBase, tableName, hivePartition, hivePartitions, METASTORE_URIS, configuration);
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
            UpdateMutation mutation = new UpdateMutation(dataBase, tableName, hivePartition, hivePartitions, METASTORE_URIS, configuration, true);
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
