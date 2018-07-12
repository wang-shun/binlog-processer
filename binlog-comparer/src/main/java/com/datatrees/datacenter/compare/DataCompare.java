package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.*;
import com.mysql.jdbc.authentication.MysqlClearPasswordPlugin;

import java.sql.SQLException;
import java.util.*;

public abstract class DataCompare implements DataCheck {
    private final int fileNum = 0;
    private final int recordNum = 8000;
    private static Properties properties=PropertiesUtility.defaultProperties();
    private static String dataBase=properties.getProperty("jdbc.database");

    @Override
    public void binLogCompare(String dest) {
    }

    @Override
    public List<Map<String, Object>> getCurrentPartitinInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_name) as files " +
                "from (select * from t_binlog_process_log where file_name='" + fileName + "') as temp group by db_instance,database_name,table_name,file_partitions having file_cnt>" + fileNum + " and sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.MYSQL.toString()),dataBase,sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }

    /**
     * find the key-value that in avroMap but not int orcMap
     *
     * @param avroMap
     * @param orcMap
     * @return
     */
    public Map<String, Long> diffCompare(Map<String, Long> avroMap, Map<String, Long> orcMap) {

        Set<Map.Entry<String, Long>> avroEntry = new HashSet<>(avroMap.entrySet());
        Set<Map.Entry<String, Long>> orcEntry = new HashSet<>(orcMap.entrySet());

        Set<Map.Entry<String, Long>> avroSet = avroMap.entrySet();
        Set<Map.Entry<String, Long>> orcSet = orcMap.entrySet();
        Map<String, Long> diffMaps = null;
        if (avroSet.removeAll(orcSet)) {
            diffMaps = new HashMap<>();
            for (Map.Entry entry : avroSet) {
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
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
        Map<String, Long> diffMaps = null;
        if (set1.retainAll(set2)) {
            diffMaps = new HashMap<>();
            for (Map.Entry entry : set1) {
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }
}
