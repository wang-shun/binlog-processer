package com.datatrees.datacenter.repair.transaction;

import com.datatrees.datacenter.operate.OperateType;
import com.google.common.collect.Lists;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.List;

/**
 * @author personalc
 */
public class TransactionOperate {
    private static HiveConf hiveConf = new HiveConf();

    private static Configuration configuration = HBaseConfiguration.create();

    // FIXME: 2018/10/9 从配置文件中读取
    private static final String METASTORE_URIS = "thrift://hadoop3:9083";

    private static final int TRANSACTION_SIZE = 10000;

    public static void repairTransaction(String dataBase, String tableName, String hivePartition, List<String> hivePartitions, String operate, Schema schema, List<GenericData.Record> genericRecordList) {
        List<List<GenericData.Record>> splitList = Lists.partition(genericRecordList, TRANSACTION_SIZE);
        if (OperateType.Create.toString().equals(operate)) {
            InsertMutation mutation = null;
            try {
                for (List<GenericData.Record> subList : splitList) {
                    mutation = new InsertMutation(dataBase, tableName, hivePartition, hivePartitions, METASTORE_URIS, configuration);
                    mutation.beginFixTransaction(schema, hiveConf);
                    for (GenericData.Record record : subList) {
                        mutation.insert(record);
                    }
                    mutation.commitTransaction();
                }
            } catch (Exception e) {
                if (mutation != null) {
                    mutation.abortTxn();
                }
                e.printStackTrace();
            }
        } else {
            UpdateMutation mutation = null;
            try {
                for (List<GenericData.Record> subList : splitList) {
                    mutation = new UpdateMutation(dataBase, tableName, hivePartition, hivePartitions, METASTORE_URIS, configuration, true);
                    mutation.beginFixTransaction(schema, hiveConf);
                    for (GenericData.Record record : subList) {
                        mutation.update(record);
                    }
                    mutation.commitTransaction();
                }
            } catch (Exception e) {
                if (mutation != null) {
                    mutation.abortTxn();
                }
                e.printStackTrace();
            }
        }
    }
}
