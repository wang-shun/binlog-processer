package com.datatrees.datacenter.table;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableInfo {
    public static final String TABLE_NAME = "streaming_warehouse_rowId2recId_tbl";
    public static final String COLUMNFAMILY = "f";
    public static final String RECORD_ID = "recordId";
    public static final String LAST_UPDATE_TIME = "update_time";
}
