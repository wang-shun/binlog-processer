import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import org.iq80.leveldb.DB;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

public class TiDBDataRepair extends BaseDataRepair {
    private Properties properties = PropertiesUtility.defaultProperties();
    private String mysqlDataBase = properties.getProperty("jdbc.database");
    private String avroHDFSPath = properties.getProperty("AVRO_HDFS_PATH");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private final String binlogTable = "t_binlog_record";


    @Override
    public void repairByTime(String startTime, String endTime, String partitionType) {
        if (startTime != null && endTime != null) {
            Date logStart = TimeUtil.strToDate(startTime, "yyyy-MM-dd HH:mm:ss");
            Date logEnd = TimeUtil.strToDate(endTime, "yyyy-MM-dd HH:mm:ss");
            String sql = "select " + CheckTable.FILE_NAME + " from " + binlogTable + " where log_start_time>=" + "'" + logStart + "'" + " and log_end_time<=" + "'" + logEnd + "'";
            try {
                List<Map<String, Object>> binlogInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), mysqlDataBase, sql);
                if (binlogInfo != null && binlogInfo.size() > 0) {
                    for (Map<String, Object> fileMap : binlogInfo) {
                        String fileName = String.valueOf(fileMap.get(CheckTable.FILE_NAME));
                        repairByFile(fileName, partitionType);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void repairByFile(String fileName, String partitionType) {
        //读取某个文件，并将所有记录解析出来重新插入到数据库
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.FILE_NAME, fileName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        try {
            List<Map<String, Object>> tableInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), mysqlDataBase, CheckTable.BINLOG_PROCESS_LOG_TABLE, whereMap);
            if (null != tableInfo && tableInfo.size() > 0) {
                for (Map<String, Object> tablePartition : tableInfo) {
                    String dbInstance = String.valueOf(tablePartition.get(CheckTable.DB_INSTANCE));
                    String databaseName = String.valueOf(tablePartition.get(CheckTable.DATA_BASE));
                    String tableName = String.valueOf(tablePartition.get(CheckTable.TABLE_NAME));
                    String filePartition = String.valueOf(tablePartition.get(CheckTable.FILE_PARTITION));
                    //先检查表是否存在
                    String tableQuerySql = "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + databaseName + "'" + " and TABLE_NAME ='" + tableName + "'";
                    List<Map<String, Object>> tableExists = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", tableQuerySql);
                    List<Set<Map.Entry<String, Object>>> upsertList;
                    List<Set<Map.Entry<String, Object>>> deleteList;

                    if (null != tableExists && tableExists.size() > 0) {
                        String filePath = avroHDFSPath + File.separator
                                + dbInstance + File.separator
                                + databaseName + File.separator
                                + tableName + File.separator
                                + filePartition + File.separator
                                + fileName + ".avro";
                        Map<String, List<Set<Map.Entry<String, Object>>>> mapList = AvroDataReader.readAllDataFromAvro(filePath);
                        upsertList = mapList.get(OperateType.Unique.toString());
                        deleteList = mapList.get(OperateType.Delete.toString());
                        if (null != upsertList && upsertList.size() > 0) {
                            batchUpsert(upsertList, databaseName, tableName);
                        }
                        if (null != deleteList && deleteList.size() > 0) {
                            batchDelete(deleteList, databaseName, tableName);
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void repairSchedule(String partitionType) {

    }

    @Override
    public void repairByRecordNum(int recordNum, String partitionType) {

    }

    /**
     * 批量删除
     *
     * @param setList   需要修复的数据列表
     * @param dataBase  数据库
     * @param tableName 数据表
     */
    private void batchDelete(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName) {
        Set<String> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
        String recordId = FieldNameOp.getFieldName(allFieldSet, idList);
        List<Object> dataList = null;
        for (Set<Map.Entry<String, Object>> recordSet : setList) {
            Iterator<Map.Entry<String, Object>> iterator = recordSet.iterator();
            dataList = new ArrayList<>();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> recordEntry = iterator.next();
                if (recordId != null && recordId.equals(recordEntry.getKey())) {
                    dataList.add(recordEntry.getValue());
                    break;
                }
            }
        }
        StringBuilder stringBuilder;
        if (dataList != null) {
            stringBuilder = new StringBuilder();
            stringBuilder
                    .append("delete from ")
                    .append(tableName)
                    .append(" where ")
                    .append(recordId)
                    .append(" in ")
                    .append("(");
            for (int i = 0; i < dataList.size(); i++) {
                Object object = dataList.get(i);
                int id = Integer.valueOf(object.toString());
                if (i < dataList.size() - 1) {
                    stringBuilder.append(id)
                            .append(",");
                } else {
                    stringBuilder.append(id);
                }
            }
            try {
                DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, stringBuilder.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private void batchUpsert(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName) {
        List<Map<String, Object>> deleteList;
        if (null != setList && setList.size() > 0) {
            deleteList = new ArrayList<>();
            for (Set<Map.Entry<String, Object>> recordSet : setList) {
                Iterator<Map.Entry<String, Object>> iterator = recordSet.iterator();
                Map<String, Object> recordMap = new HashMap<>(recordSet.size());
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> recordEntry = iterator.next();
                    recordMap.put(recordEntry.getKey(), recordEntry.getValue());
                }
                deleteList.add(recordMap);
            }
            try {
                DBUtil.insertAll(DBServer.DBServerType.TIDB.toString(), dataBase, tableName, deleteList);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
