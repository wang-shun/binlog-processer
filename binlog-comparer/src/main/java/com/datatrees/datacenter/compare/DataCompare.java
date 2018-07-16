package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.table.CheckTable;
import com.mysql.jdbc.authentication.MysqlClearPasswordPlugin;
import org.tukaani.xz.check.Check;

import java.sql.SQLException;
import java.util.*;

public abstract class DataCompare implements DataCheck {
    private final int fileNum = 0;
    private final int recordNum = 2000;
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static String dataBase = properties.getProperty("jdbc.database");
    private static String processLogTable = CheckTable.BINLOG_PROCESS_LOG_TABLE;

    @Override
    public void binLogCompare(String dest) {
    }

    public List<Map<String, Object>> getCurrentPartitionInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_name) as files " +
                "from (select * from " + processLogTable + " where file_name='" + fileName + "') as temp group by db_instance,database_name,table_name,file_partitions having file_cnt>" + fileNum + " and sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.MYSQL.toString()), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    public List<Map<String, Object>> getCurrentTableInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        //String maxLen="SET GLOBAL group_concat_max_len = 102400";
        String sql = "select db_instance,database_name,table_name,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_partitions) as partitions from " +
                " (select * from " + processLogTable + " where file_name= '" + fileName + "') as temp group by db_instance,database_name,table_name having sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.MYSQL.toString()), dataBase, sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
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
        if (srcMap.size()>0  && destMap.size()>0) {
            if (avroSet.removeAll(orcSet)) {
                diffMaps = new HashMap<>();
                for (Map.Entry entry : avroSet) {
                    diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
                }
            }
        } else {
            if (srcMap.size()>0) {
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
     * @param Map1
     * @param Map2
     * @return
     */
    public Map<String, Long> retainCompare(Map<String, Long> Map1, Map<String, Long> Map2) {

        Set<Map.Entry<String, Long>> set1 = Map1.entrySet();
        Set<Map.Entry<String, Long>> set2 = Map2.entrySet();
        Map<String, Long> diffMaps = new HashMap<>();
        if (set1.retainAll(set2)) {
            for (Map.Entry entry : set1) {
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }
}
