package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.transfer.bean.DownloadStatus;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.utility.DBInstanceUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public abstract class BinlogFileTransfer implements TaskRunner {

    private static Logger LOG = LoggerFactory.getLogger(BinlogFileTransfer.class);
    public Properties properties = PropertiesUtility.defaultProperties();
    public String dataBase = properties.getProperty("jdbc.database", "binlog");
    public final String HDFS_PATH = properties.getProperty("HDFS_PATH");
    public int retryTimes = Integer.parseInt(properties.getProperty("process.check.schedule.task.retry", "1"));


    @Override
    public void process() {

    }

    void transfer() {

    }

    /**
     * 检查下载表中是否有下载完但没有发往处理表中的数据
     */
    public void checkDataConsistency() {
        String sql = "select r.id,r.db_instance,r.file_name,r.bak_instance_id from " + TableInfo.BINLOG_TRANS_TABLE + " as r left join " + TableInfo.BINLOG_PROC_TABLE + " as p on r.db_instance=p.db_instance and r.file_name=p.file_name where p.id is null and r.status=1";
        List<Map<String, Object>> sendFailedRecord;
        try {
            sendFailedRecord = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
            if (sendFailedRecord.size() > 0) {
                Iterator<Map<String, Object>> iterator = sendFailedRecord.iterator();
                Map<String, Object> recordMap;
                while (iterator.hasNext()) {
                    recordMap = iterator.next();
                    String dbInstance = String.valueOf(recordMap.get(TableInfo.DB_INSTANCE));
                    String fileName = String.valueOf(recordMap.get(TableInfo.FILE_NAME));
                    String backInstanceId = String.valueOf(recordMap.get(TableInfo.BAK_INSTANCE_ID));
                    String path;
                    String mysqlURL;
                    Map<String, Object> map = new HashMap<>(5);

                    if (null != backInstanceId || !"".equals(backInstanceId)) {
                        path = HDFS_PATH + File.separator + dbInstance + File.separator + backInstanceId + File.separator + fileName;
                        mysqlURL = DBInstanceUtil.getConnectString(dbInstance);
                        map.put(TableInfo.BAK_INSTANCE_ID, backInstanceId);
                    } else {
                        path = HDFS_PATH + File.separator + dbInstance + File.separator + fileName;
                        mysqlURL = dbInstance;
                    }
                    String identity = dbInstance + TableInfo.INSTANCE_FILE_SEP + fileName;
                    TaskDispensor.defaultDispensor().dispense(new Binlog(path, identity, mysqlURL));

                    map.put(TableInfo.FILE_NAME, fileName);
                    map.put(TableInfo.DB_INSTANCE, dbInstance);
                    map.put(TableInfo.PROCESS_START, TimeUtil.stampToDate(System.currentTimeMillis()));
                    try {
                        DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, map);
                    } catch (SQLException e) {
                        LOG.error("insert " + fileName + "to t_binlog_process failed");
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 对解析或者下载错误的文件重新下载
     */
    public void processErrorFile(String sql) {
        try {
            List<Map<String, Object>> errorData = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql);
            if (!errorData.isEmpty()) {
                for (Map<String, Object> objectMap : errorData) {
                    String fileName = (String) objectMap.get(TableInfo.FILE_NAME);
                    String dbInstance = (String) objectMap.get(TableInfo.DB_INSTANCE);
                    String bakInstanceId = (String) objectMap.get(TableInfo.BAK_INSTANCE_ID);
                    String path;
                    if (null != bakInstanceId || !"".equals(bakInstanceId)) {
                        path = HDFS_PATH + File.separator + dbInstance + File.separator + bakInstanceId + File.separator + fileName;
                    } else {
                        path = HDFS_PATH + File.separator + dbInstance + File.separator + fileName;
                    }
                    boolean status = HDFSFileUtility.checkAndDel(path);
                    boolean fileExists = HDFSFileUtility.getFileSystem(path).exists(new Path(path));
                    if (status || fileExists) {
                        LOG.info("delete file: " + fileName + " from HDFS success");
                        Map<String, Object> whereMap = new HashMap<>();
                        whereMap.put(TableInfo.FILE_NAME, fileName);
                        whereMap.put(TableInfo.DB_INSTANCE, dbInstance);
                        DBUtil.delete(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_PROC_TABLE, whereMap);
                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put(TableInfo.DOWN_STATUS, DownloadStatus.UNCOMPLETED.getValue());
                        valueMap.put(TableInfo.DOWN_SIZE, null);
                        valueMap.put(TableInfo.REQUEST_END, null);
                        DBUtil.update(DBServer.DBServerType.MYSQL.toString(), dataBase, TableInfo.BINLOG_TRANS_TABLE, valueMap, whereMap);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getSQLState(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 返回传输未完成的binlog文件信息
     */
    public List<Map<String, Object>> getUnCompleteTrans(String instanceStr) {
        List<Map<String, Object>> resultList = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("select")
                    .append(" ")
                    .append("distinct")
                    .append(" ")
                    .append(TableInfo.DOWN_START_TIME)
                    .append(",")
                    .append(TableInfo.DOWN_END_TIME)
                    .append(" ")
                    .append("from")
                    .append(" ")
                    .append(TableInfo.BINLOG_TRANS_TABLE)
                    .append(" ")
                    .append("where")
                    .append(" ")
                    .append(TableInfo.DOWN_STATUS)
                    .append("=")
                    .append(DownloadStatus.UNCOMPLETED.getValue())
                    .append(" ")
                    .append("and")
                    .append(" ")
                    .append(TableInfo.DB_INSTANCE)
                    .append(" ")
                    .append("in")
                    .append(" ")
                    .append("(")
                    .append(instanceStr)
                    .append(")")
                    .append(" ")
                    .append("order by")
                    .append(" ")
                    .append(TableInfo.DOWN_START_TIME);

            resultList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), dataBase, sql.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return resultList;
    }
}
