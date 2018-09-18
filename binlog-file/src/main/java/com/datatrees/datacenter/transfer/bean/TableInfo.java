package com.datatrees.datacenter.transfer.bean;

/**
 * @author personalc
 */
public class TableInfo {
    public static final String BINLOG_DATABASE="binlog";
    public static final String BINLOG_TRANS_TABLE = "t_binlog_record";
    public static final String BINLOG_PROC_TABLE = "t_binlog_process";
    public static final String BINLOG_LOCAL_CHECK_TABLE="";
    public static final String RECORD_ID = "id";
    public static final String BATCH_ID = "batch_id";
    public static final String FILE_NAME = "file_name";
    public static final String DB_INSTANCE = "db_instance";
    public static final String BAK_INSTANCE_ID = "bak_instance_id";
    public static final String LOG_START_TIME = "log_start_time";
    public static final String LOG_END_TIME = "log_end_time";
    public static final String DOWN_START_TIME = "down_start";
    public static final String DOWN_END_TIME = "down_end";
    public static final String DOWN_LINK = "down_link";
    public static final String REQUEST_START = "request_start";
    public static final String REQUEST_END = "request_end";
    public static final String HOST = "host";
    public static final String DOWN_STATUS = "status";
    public static final String UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String COMMON_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String PROCESS_START = "process_start";
    public static final String PROCESS_END = "process_end";
    public static final String RETRY_TIMES = "retry_times";
    public static final String PROCESS_STATUS = "status";
    public static final String FILE_SIZE = "file_size";
    public static final String DOWN_SIZE = "down_size";
    public static final String INSTANCE_FILE_SEP="_";
}
