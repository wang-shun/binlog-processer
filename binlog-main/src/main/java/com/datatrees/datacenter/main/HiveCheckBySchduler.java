package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BatchGetFromHBase;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.rabbitmq.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author personalc
 */
public class HiveCheckBySchduler {
    private static Logger LOG = LoggerFactory.getLogger(HiveCheckBySchduler.class);
    private static final String HBASE_ROKEY_FIELD = "hbase_rowkey";
    static ProducerTask producer = new ProducerTask("hive_check_test");
    private static  String databases=PropertiesUtility.defaultProperties().getProperty("databases.need.check");


    public static void main(String[] args) {
        Runnable runnable = () -> {
            //往前推一段时间
            String sql = "select db_instance,database_name,table_name,file_partitions,file_name,create_date,type from t_binlog_process_log where create_date<now()-interval 1000 minute and create_date>now()-interval 1500 minute and repair_status=0 and file_partitions<> 'null' and type='create'";
            String dataBaseAssemble = assembleDatabase(databases);
            sql+=dataBaseAssemble;
            try {
                List<Map<String, Object>> partitionInfos = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), "binlog", sql);
                if (partitionInfos != null && partitionInfos.size() > 0) {
                    List<String> rowKeyList = new ArrayList<>(partitionInfos.size());
                    for (Map<String, Object> tableMap : partitionInfos) {
                        String hiveTableLastUpdate = tableMap.get(CheckTable.DATA_BASE).toString() +
                                "." + tableMap.get(CheckTable.TABLE_NAME)
                                + "_" + PartitionUtility.getHivePartition(PartitionUtility.getDateMap(String.valueOf(tableMap.get(CheckTable.FILE_PARTITION))));
                        rowKeyList.add(hiveTableLastUpdate);
                        tableMap.put(HBASE_ROKEY_FIELD, hiveTableLastUpdate);
                    }
                    List<String> rowKeyListFilter = rowKeyList.stream().distinct().collect(Collectors.toList());
                    Map<String, byte[]> recordLastUpdateTime = BatchGetFromHBase.getBatchDataFromHBase(rowKeyListFilter, "streaming_warehouse_system_conf", "f", "stream_update_time");
                    System.out.println("recordLastUpdateTime:" + recordLastUpdateTime.size());
                    Map<String, Long> recordLastUpdateTimeLong = new HashMap<>(recordLastUpdateTime.size());
                    for (Map.Entry<String, byte[]> recordTimeMap : recordLastUpdateTime.entrySet()) {
                        recordLastUpdateTimeLong.put(recordTimeMap.getKey(), Long.valueOf(Bytes.toString(recordTimeMap.getValue())));
                    }
                    List<Map<String, Object>> tableInfofilter = partitionInfos.stream().filter(x -> recordLastUpdateTime.keySet().contains(String.valueOf(x.get(HBASE_ROKEY_FIELD)))).collect(Collectors.toList());
                    System.out.println("tableInfofilter size:" + tableInfofilter.size());
                    List<Map<String, Object>> tableInfoLast = tableInfofilter
                            .stream()
                            .filter(x -> (null != recordLastUpdateTimeLong.get(x.get(HBASE_ROKEY_FIELD)) && (recordLastUpdateTimeLong.get(x.get(HBASE_ROKEY_FIELD))
                                    - TimeUtil.strToDate(x.get("create_date").toString(), "yyyy-MM-dd HH:mm:ss").getTime() > 0))).collect(Collectors.toList());

                    System.out.println("tableInfoLast size:" + tableInfoLast.size());
                    if (tableInfoLast.size() > 0) {
                        for (Map<String, Object> map : tableInfoLast) {
                            queueAndDbProcess(map);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable, 0, 2, TimeUnit.MINUTES);
    }

    private static String assembleDatabase(String databases) {
        if(null!=databases){
            String[] dataBaseArr=databases.split(",");
            StringBuilder dataBaseStr=new StringBuilder();
            dataBaseStr.append(" and database_name in").append("(");
            for (int i=0;i<dataBaseArr.length;i++) {
                if(i!=dataBaseArr.length-1) {
                    dataBaseStr.append("'")
                            .append(dataBaseArr[i])
                            .append("'")
                            .append(",");
                }else{
                    dataBaseStr.append("'")
                            .append(dataBaseArr[i])
                            .append("'")
                    .append(")");
                }
            }
            return dataBaseStr.toString();
        }
        return "";
    }

    /**
     * 将代检查数据发队列并更新数据库状态
     *
     * @param map 代检查map数据
     */
    private static void queueAndDbProcess(Map<String, Object> map) {
        String dbInstance = String.valueOf(map.get(CheckTable.DB_INSTANCE));
        String dataBase = String.valueOf(map.get(CheckTable.DATA_BASE));
        String tableName = String.valueOf(map.get(CheckTable.TABLE_NAME));
        String partitions = String.valueOf(map.get(CheckTable.FILE_PARTITION));
        String partitionType = String.valueOf(map.get(CheckTable.PARTITION_TYPE));
        String fileName = String.valueOf(map.get(CheckTable.FILE_NAME));
        String stringBuilder = dbInstance +
                ":" +
                dataBase +
                ":" +
                tableName +
                ":" +
                partitions +
                ":" +
                partitionType +
                ":" +
                fileName;
        producer.sendMessage(stringBuilder);
        map.remove(HBASE_ROKEY_FIELD);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 99);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, "t_binlog_process_log", valueMap, map);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
