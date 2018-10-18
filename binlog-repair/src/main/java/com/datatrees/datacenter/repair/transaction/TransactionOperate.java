package com.datatrees.datacenter.repair.transaction;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
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
import java.util.Properties;

/**
 * @author personalc
 */
public class TransactionOperate {
    private static HiveConf hiveConf = new HiveConf();

    private static Configuration configuration = HBaseConfiguration.create();
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String META_STORE_URIS = properties.getProperty("hive.metastore.uri");
    private static final int TRANSACTION_SIZE = Integer.valueOf(properties.getProperty("id.list.max"));

    public static void repairTransaction(String dataBase, String tableName, String hivePartition, List<String> hivePartitions, String operate, Schema schema, List<GenericData.Record> genericRecordList) {
        List<List<GenericData.Record>> splitList = Lists.partition(genericRecordList, TRANSACTION_SIZE);
        if (OperateType.Create.toString().equals(operate)) {
            InsertMutation mutation = null;
            try {
                for (List<GenericData.Record> subList : splitList) {
                    mutation = new InsertMutation(dataBase, tableName, hivePartition, hivePartitions, META_STORE_URIS, configuration);
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
                    mutation = new UpdateMutation(dataBase, tableName, hivePartition, hivePartitions, META_STORE_URIS, configuration, true);
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
