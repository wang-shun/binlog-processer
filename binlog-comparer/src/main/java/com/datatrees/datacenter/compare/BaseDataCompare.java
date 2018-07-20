package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.table.CheckTable;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseDataCompare implements DataCheck {

    private static Properties properties = PropertiesUtility.defaultProperties();
    private final int fileNum = Integer.valueOf(properties.getProperty("FILE_NUM"));
    private final int recordNum = Integer.valueOf(properties.getProperty("RECORD_NUM"));
    private static String dataBase = properties.getProperty("jdbc.database");
    private static String processLogTable = CheckTable.BINLOG_PROCESS_LOG_TABLE;
    final String AVRO_HDFS_PATH = properties.getProperty("AVRO_HDFS_PATH");

    @Override
    public void binLogCompare(String dest) {
    }

    @Override
    public void binLogCompare(String database, String table, String partition,String partitionType) {

    }

    List<Map<String, Object>> getCurrentPartitionInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_name) as files " +
                "from (select * from " + processLogTable + " where file_name=" + "'" + fileName + "'" + ") as temp group by db_instance,database_name,table_name,file_partitions having file_cnt>" + fileNum + " and sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    List<Map<String, Object>> getCurrentTableInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        //String maxLen="SET GLOBAL group_concat_max_len = 102400";
        String sql = "select db_instance,database_name,table_name,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_partitions) as partitions from " +
                " (select * from " + processLogTable + " where file_name=" + "'" + fileName + "'" + ") as temp group by db_instance,database_name,table_name having sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    List<Map<String, Object>> getSpecifiedDateTableInfo(String instance, String tableName, String partitions, String partitionType) {
        List<Map<String, Object>> partitionInfo = null;
        Map<String, String> whereMap = new HashMap<>();
        whereMap.put("database_name", instance);
        whereMap.put("table_name", tableName);
        whereMap.put("file_partitions", partitions);
        whereMap.put("type", partitionType);
        whereMap.values().remove("");
        List<Map.Entry<String, String>> map2List;
        map2List = whereMap.entrySet().stream().collect(Collectors.toList());
        StringBuilder whereExpress = new StringBuilder();
        if (whereMap.size() > 0) {
            whereExpress.append(" where ");
            for (int i = 0; i < map2List.size(); i++) {
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
        try {
            String sql = "select db_instance,database_name,table_name,GROUP_CONCAT(file_name) as files,file_partitions from " + processLogTable + " " + whereExpress.toString() + " group by database_name,table_name";
            partitionInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partitionInfo;
    }

    /**
     * find the key-value that in avroMap but not int orcMap
     *
     * @param srcMap
     * @param destMap
     * @return
     */
    public Map<String, Long> diffCompare(Map<String, Long> srcMap, Map<String, Long> destMap) {

        Set<Map.Entry<String, Long>> avroSet = srcMap.entrySet();
        Set<Map.Entry<String, Long>> orcSet = destMap.entrySet();
        Map<String, Long> diffMaps = null;
        if (srcMap.size() > 0 && destMap.size() > 0) {
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
        return diffMaps;
    }

    /**
     * find the key-value that both in Map1 and Map2
     *
     * @param map1
     * @param map2
     * @return
     */
    public Map<String, Long> retainCompare(Map<String, Long> map1, Map<String, Long> map2) {

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
}
