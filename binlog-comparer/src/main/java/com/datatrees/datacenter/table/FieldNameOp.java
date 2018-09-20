package com.datatrees.datacenter.table;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;


public class FieldNameOp {
    private static Logger LOG = LoggerFactory.getLogger(FieldNameOp.class);

    public static String getFieldName(Collection<Object> allFieldName, List<String> configField) {
        Set<String> fieldSets = configField.stream().collect(Collectors.toSet());
        if (null != allFieldName && allFieldName.size() > 0) {
            allFieldName.stream().collect(Collectors.toSet());
            if (fieldSets.retainAll(allFieldName)) {
                if (fieldSets.size() > 0) {
                    return String.valueOf(fieldSets.toArray()[0]);
                }
            }
        }
        return null;
    }
    public static String getFieldName(Set<String> allFieldName, List<String> configField) {
        Set<String> fieldSets = configField.stream().collect(Collectors.toSet());
        if (null != allFieldName && allFieldName.size() > 0) {
            if (fieldSets.retainAll(allFieldName)) {
                if (fieldSets.size() > 0) {
                    return String.valueOf(fieldSets.toArray()[0]);
                }
            }
        }
        return null;
    }

    public static List<String> getConfigField(String fieldName) {
        String allName = PropertiesUtility.defaultProperties().getProperty(fieldName);
        return new ArrayList<>(asList(allName.split(",")));
    }

    /**
     * 从数据库中获取表字段
     *
     * @param dataBase  数据库
     * @param tableName 表
     * @return 字段集合
     */
    public static Collection<Object> getAllFieldName(String dataBase, String tableName) {
        try {
            String tableQuerySql = "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + dataBase + "'" + " and TABLE_NAME ='" + tableName + "'";
            List<Map<String, Object>> tableExists = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", tableQuerySql);
            if (null != tableExists && tableExists.size() > 0) {
                String tableFieldSql = "select COLUMN_NAME from INFORMATION_SCHEMA.Columns where table_name='" + tableName + "' and table_schema='" + dataBase + "'";
                List<Map<String, Object>> mapList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, tableFieldSql);
                if (null != mapList) {
                    List<Object> columns = mapList.stream().map(Map::values).flatMap(values -> values.stream())
                            .collect(Collectors.toList());
                    return columns;
                }
            } else {
                LOG.info("Table " + dataBase + "." + tableName + " doesn't exist!");
                return null;
            }
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return null;
    }
}
