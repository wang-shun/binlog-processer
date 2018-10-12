package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.sql.SQLException;
import java.util.*;

public abstract class BaseDataCompare implements DataCheck {

    private static Properties properties = PropertiesUtility.defaultProperties();
    private final int fileNum = Integer.parseInt(properties.getProperty("FILE_NUM","0"));
    private final int recordNum = Integer.parseInt(properties.getProperty("RECORD_NUM","0"));
    private static String dataBase = properties.getProperty("jdbc.database","binlog");
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
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,file_name as files " +
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
     * @param tableName
     * @param partitions
     * @param partitionType
     * @return
     */
    public static List<Map<String, Object>> getSpecifiedDateTableInfo(String dataBase, String tableName, String partitions, String partitionType) {
        List<Map<String, Object>> partitionInfo = null;
        Map<String, Object> whereMap = new HashMap<>();
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.FILE_PARTITION, partitions);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
        try {
            String sql = "select db_instance,database_name,table_name,GROUP_CONCAT(file_name) as files,file_partitions from " + processLogTable + " " + whereExpress.toString() + " group by database_name,table_name,file_partitions";
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
    public static Map<String, Long> diffCompare(Map<String, Long> srcMap, Map<String, Long> destMap) {
        Map<String, Long> diffMaps = null;
        if (srcMap != null && srcMap.size() > 0) {
            if (destMap != null && destMap.size() > 0) {
                MapDifference<String, Long> diff=Maps.difference(srcMap,destMap);
                Set<String> keysOnlyInSource = diff.entriesOnlyOnLeft().keySet();
                if(keysOnlyInSource!=null&&keysOnlyInSource.size()>0) {
                    diffMaps=new HashMap<>(keysOnlyInSource.size());
                    for (String id : keysOnlyInSource) {
                        diffMaps.put(id, srcMap.get(id));
                    }
                }
            } else {
                srcMap.size();
                return srcMap;
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

}
