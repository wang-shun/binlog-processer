package com.datatrees.datacenter.repair.tidb;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.table.FieldNameOp;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;

class DataBaseHandler {

    private static final List<String> ID_LIST = FieldNameOp.getConfigField("id");
    private static final List<String> UPDATE_TIME_LIST = FieldNameOp.getConfigField("update");

    static List<Object> getTimeFields(String tableName, String dataBase) throws SQLException {
        List<Map<String, Object>> columnTypes = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", "select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_name = '" + tableName + "' and table_schema = '" + dataBase + "'");
        List<Object> timeColumns = new ArrayList<>();
        for (Map<String, Object> columnType : columnTypes) {
            String columnName = (String) columnType.get("COLUMN_NAME");
            String dataType = (String) columnType.get("DATA_TYPE");
            if ("date".equals(dataType) || "timestamp".equals(dataType)) {
                timeColumns.add(columnName);
            }
        }
        return timeColumns;
    }

    private static List<Map<String, Object>> assembleData(List<Set<Map.Entry<String, Object>>> setList, Collection<Object> timeColumns) {
        List<Map<String, Object>> dataMapList = new ArrayList<>();
        for (Set<Map.Entry<String, Object>> recordSet : setList) {
            Iterator<Map.Entry<String, Object>> iterator = recordSet.iterator();
            Map<String, Object> recordMap = new HashMap<>(recordSet.size());
            while (iterator.hasNext()) {
                Map.Entry<String, Object> recordEntry = iterator.next();
                String key = recordEntry.getKey();
                Object value = recordEntry.getValue();
                if (value != null) {
                    if (timeColumns.contains(key)) {
                        String valueStr = String.valueOf(value);
                        if (valueStr.length() == 10) {
                            long longTimeStamp = Long.valueOf(valueStr) * 1000;
                            recordMap.put(key, TimeUtil.stampToDate(longTimeStamp));
                        } else {
                            Long longTimeStamp = Long.valueOf(valueStr);
                            recordMap.put(key, TimeUtil.stampToDate(longTimeStamp));
                        }
                    } else {
                        if (value instanceof JSONObject) {
                            String valueStr = null;
                            try {
                                valueStr = new String(((JSONObject) value).getString("bytes").getBytes("iso8859-1"), "utf-8");
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                            recordMap.put(key, valueStr);
                        } else {
                            recordMap.put(key, value);
                        }
                    }
                } else {
                    recordMap.put(key, null);
                }
            }
            dataMapList.add(recordMap);
        }
        return dataMapList;
    }

    /**
     * 批量删除
     *
     * @param list      需要修复的数据列表
     * @param dataBase  数据库
     * @param tableName 数据表
     */
    static void batchDelete(List<Set<Map.Entry<String, Object>>> list, String dataBase, String tableName) {
        Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
        String recordId = FieldNameOp.getFieldName(allFieldSet, ID_LIST);
        List<Object> dataList = null;
        for (Set<Map.Entry<String, Object>> recordSet : list) {
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
                int id = Integer.parseInt(object.toString());
                if (i < dataList.size() - 1) {
                    stringBuilder.append("'")
                            .append(id)
                            .append("'")
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

    static void batchInsert(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName, List<Object> timeColumns) {
        List<Map<String, Object>> inSertList;
        if (null != setList && setList.size() > 0) {
            inSertList = assembleData(setList, timeColumns);
            try {
                DBUtil.insertAll(DBServer.DBServerType.TIDB.toString(), dataBase, tableName, inSertList);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    protected static void batchUpsert(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName, Collection<Object> timeColumns) {
        List<Map<String, Object>> updateList;
        if (null != setList && setList.size() > 0) {
            updateList = assembleData(setList, timeColumns);
            String lastUpdateColumn = FieldNameOp.getFieldName(timeColumns, UPDATE_TIME_LIST);
            if (lastUpdateColumn != null) {
                try {
                    DBUtil.upsertAll(DBServer.DBServerType.TIDB.toString(), dataBase, tableName, updateList, lastUpdateColumn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
