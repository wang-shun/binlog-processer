package com.datatrees.datacenter.compare;

import avro.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.HBaseHelper;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public abstract class BaseDataCompare implements DataCheck {

    private static Properties properties = PropertiesUtility.defaultProperties();
    private final int fileNum = Integer.parseInt(properties.getProperty("FILE_NUM"));
    private final int recordNum = Integer.parseInt(properties.getProperty("RECORD_NUM"));
    private static String dataBase = properties.getProperty("jdbc.database");
    private static String processLogTable = CheckTable.BINLOG_PROCESS_LOG_TABLE;
    final String AVRO_HDFS_PATH = properties.getProperty("AVRO_HDFS_PATH");

    @Override
    public void binLogCompare(String dest, String type) {
    }

    @Override
    public void binLogCompare(String database, String table, String partition, String partitionType) {

    }

    /**
     * 获取某个binlog解析后的avro文件根据分区分组后的情况
     *
     * @param fileName 文件名
     * @return 分区信息
     */
    List<Map<String, Object>> getCurrentPartitionInfo(String fileName, String type) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_name) as files " +
                "from (select * from " + processLogTable + " where type=" + "'" + type + "'" + " and file_name=" + "'" + fileName + "'" + ") as temp group by db_instance,database_name,table_name,file_partitions having file_cnt>" + fileNum + " and sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    /**
     * 查询某个binlo解析出来的avro文件根据表分组的情况
     *
     * @param fileName
     * @param type
     * @return
     */
    List<Map<String, Object>> getCurrentTableInfo(String fileName, String type) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_name,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_partitions) as partitions from " +
                " (select * from " + processLogTable + " where type=" + "'" + type + "'" + " and file_name=" + "'" + fileName + "'" + " ) as temp group by db_instance,database_name,table_name having sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    /**
     * 查询指定库、表、分区以及分区类型的信息
     *
     * @param instance
     * @param tableName
     * @param partitions
     * @param partitionType
     * @return
     */
    public static List<Map<String, Object>> getSpecifiedDateTableInfo(String instance, String tableName, String partitions, String partitionType) {
        List<Map<String, Object>> partitionInfo = null;
        Map<String, String> whereMap = new HashMap<>();
        whereMap.put(CheckTable.DATA_BASE, instance);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.FILE_PARTITION, partitions);
        whereMap.put("type", partitionType);
        StringBuilder whereExpress = getStringBuilder(whereMap);
        try {
          /*  String maxLen = "SET GLOBAL group_concat_max_len = 102400";
            DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, maxLen);*/
            String sql = "select db_instance,database_name,table_name,GROUP_CONCAT(file_name) as files,file_partitions from " + processLogTable + " " + whereExpress.toString() + " group by database_name,table_name";
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partitionInfo;
    }

    /**
     * 组装where语句
     *
     * @param whereMap
     * @return
     */
    public static StringBuilder getStringBuilder(Map<String, String> whereMap) {
        whereMap.values().remove("");
        List<Map.Entry<String, String>> map2List;
        map2List = new ArrayList<>(whereMap.entrySet());
        StringBuilder whereExpress = new StringBuilder();
        if (whereMap.size() > 0) {
            whereExpress.append(" where ");
            for (int i = 0, length = map2List.size(); i < length; i++) {
                Map.Entry<String, String> express = map2List.get(i);
                whereExpress
                        .append(express.getKey())
                        .append("=")
                        .append("'")
                        .append(express.getValue())
                        .append("'")
                        .append(" ");
                if (i < map2List.size() - 1) {
                    whereExpress
                            .append(" ")
                            .append("and")
                            .append(" ");
                }
            }
        }
        return whereExpress;
    }

    /**
     * find the key-value that in avroMap but not int orcMap
     *
     * @param srcMap
     * @param destMap
     * @return
     */
    public static Map<String, Long> diffCompare(Map<String, Long> srcMap, Map<String, Long> destMap) {
        Map<String, Long> diffMaps = null;
        if (srcMap != null && srcMap.size() > 0) {
            if (destMap != null && destMap.size() > 0) {
                Set<Map.Entry<String, Long>> avroSet = srcMap.entrySet();
                Set<Map.Entry<String, Long>> orcSet = destMap.entrySet();
                if (avroSet.removeAll(orcSet)) {
                    diffMaps = new HashMap<>();
                    for (Map.Entry entry : avroSet) {
                        diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
                    }
                }
            } else {
                if (srcMap.size() > 0) {
                    return srcMap;
                } else {
                    return null;
                }
            }
        }
        return diffMaps;
    }

    /**
     * find the key-value that both in Map1 and Map2
     *
     * @param map1
     * @param map2
     * @return
     */
    public static Map<String, Long> retainCompare(Map<String, Long> map1, Map<String, Long> map2) {
        Set<Map.Entry<String, Long>> set1 = map1.entrySet();
        Set<Map.Entry<String, Long>> set2 = map2.entrySet();
        Map<String, Long> diffMaps = new HashMap<>();
        if (set1.retainAll(set2)) {
            for (Map.Entry entry : set1) {
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }

    /**
     * 根据rowkey批量查询数据
     *
     * @param idList       id列表
     * @param tableName    hbase表名
     * @param columnFamily 列族
     * @param column       列
     * @return Map
     */
    public static Map<String, Long> getBatchDataFromHBase(List<String> idList, String tableName, String columnFamily, String column) {
        Map<String, Long> resultMap = null;
        if (null != idList && idList.size() > 0) {
            Table table = HBaseHelper.getTable(tableName);
            List<Get> gets = new ArrayList<>();
            for (String anIdList : idList) {
                Get get = new Get(Bytes.toBytes(anIdList));
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                gets.add(get);
            }
            try {
                Result[] results = table.get(gets);
                if (null != results && results.length > 0) {
                    resultMap = new HashMap<>();
                    for (Result result : results) {
                        if (null != result) {
                            String rowKey = Bytes.toString(result.getRow());
                            long time = Bytes.toLong(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)));
                            resultMap.put(rowKey, time);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return resultMap;
    }

    static class BatchSearchCallable implements Callable<Map<String, Long>> {
        private List<String> ids;
        private String tableName;
        private String columnFamily;
        private String column;

        BatchSearchCallable(List<String> keys, String tableName, String columnFamily, String column) {
            this.ids = keys;
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.column = column;
        }

        @Override
        public Map<String, Long> call() {
            return BaseDataCompare.getBatchDataFromHBase(ids, tableName, columnFamily, column);
        }
    }

    /**
     * 多线程批量查询
     *
     * @param idList       需要查询的rowKey列表
     * @param tableName    表名
     * @param columnFamily 列簇
     * @param column       列名
     * @return map
     */
    public Map<String, Long> parrallelBatchSearch(List<String> idList, String tableName, String columnFamily, String column) {
        Map<String, Long> dataMap = new HashMap<>();
        int parallel = (Runtime.getRuntime().availableProcessors() + 1) * 2;
        List<List<String>> batchIdList;
        if (null != idList && idList.size() > 0) {
            if (idList.size() < parallel) {
                batchIdList = new ArrayList<>(1);
                batchIdList.add(idList);
            } else {
                batchIdList = new ArrayList<>(parallel);
                List<String> lst;
                for (int i = 0; i < parallel; i++) {
                    lst = new ArrayList<>();
                    batchIdList.add(lst);
                }
                for (int i = 0; i < idList.size(); i++) {
                    batchIdList.get(i % parallel).add(idList.get(i));
                }
            }
            List<Future<Map<String, Long>>> futures = new ArrayList<>(parallel);
            ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
            builder.setNameFormat("parallelBatchQuery");
            ThreadFactory factory = builder.build();
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, factory);
            for (List<String> keys : batchIdList) {
                Callable<Map<String, Long>> callable = new BatchSearchCallable(keys, tableName, columnFamily, column);
                FutureTask<Map<String, Long>> future = (FutureTask<Map<String, Long>>) executor.submit(callable);
                futures.add(future);
            }
            executor.shutdown();
            try {
                boolean stillRuning = !executor.awaitTermination(30000, TimeUnit.MILLISECONDS);
                if (stillRuning) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            }
            for (Future f : futures) {
                try {
                    if (f.get() != null) {
                        dataMap.putAll((Map<String, Long>) f.get());
                    }
                } catch (InterruptedException e) {
                    try {
                        Thread.currentThread().interrupt();
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return dataMap;
    }
}
