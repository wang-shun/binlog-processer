package com.datatrees.datacenter.utility;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckDBUtil {

    private static Logger LOG = LoggerFactory.getLogger(CheckDBUtil.class);
    private static final String ID_LIST_MAX = PropertiesUtility.defaultProperties().getProperty("ID_LIST_MAX", "1000");

    public static void resultInsert(CheckResult result, Map<String, Long> afterComp) {

        if (afterComp != null && afterComp.size() > 0) {
            int dataSize = afterComp.size();
            LOG.info("record size is :" + dataSize);
            Map<String, Object> dataMap = new HashMap<>(11);
            dataMap.put(CheckTable.FILE_NAME, result.getFileName());
            dataMap.put(CheckTable.DB_INSTANCE, result.getDbInstance());
            dataMap.put(CheckTable.DATA_BASE, result.getDataBase());
            dataMap.put(CheckTable.TABLE_NAME, result.getTableName());
            dataMap.put(CheckTable.PARTITION_TYPE, result.getPartitionType());
            dataMap.put(CheckTable.FILE_PARTITION, result.getFilePartition());
            dataMap.put(CheckTable.OP_TYPE, result.getOpType());
            dataMap.put(CheckTable.FILES_PATH, result.getFilesPath());
            dataMap.put(CheckTable.DATA_COUNT, dataSize);
            int max_size = Integer.parseInt(ID_LIST_MAX);
            if (dataSize < max_size) {
                dataMap.put(CheckTable.ID_LIST, afterComp.keySet().toString());
            } else {
                List<String> idList = new ArrayList<>(afterComp.keySet());
                LOG.info("the record size is :" + dataSize);
                dataMap.put(CheckTable.ID_LIST, idList.subList(0, max_size).toString());
                LOG.info("insert size of database is :" + max_size);
            }
            long currentTime = System.currentTimeMillis();
            dataMap.put(CheckTable.LAST_UPDATE_TIME, TimeUtil.stampToDate(currentTime));
            try {
                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, "t_binlog_check_hive", dataMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info("no error record find from : " + result.getDataBase() + "." + result.getTableName());
        }
    }
}
