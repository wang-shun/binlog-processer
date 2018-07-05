package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBUtil;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class DataCompare implements DataCheck {
    private final int fileNum = 5;
    private final int recordNum = 100;

    @Override
    public void binLogCompare(String src, String dest) {
    }

    @Override
    public List<Map<String, Object>> getCurrentPartitinInfo(String fileName) {
        List<Map<String, Object>> partitionInfo = null;
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sum_cnt,GROUP_CONCAT(file_name) as files " +
                "from (select * from t_binlog_process_log where file_name='" + fileName + "' and status=0) as temp group by db_instance,database_name,table_name,file_partitions having file_cnt>" + fileNum + " and sum_cnt>" + recordNum;
        try {
            partitionInfo = DBUtil.query(sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }
}
