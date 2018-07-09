package com.datatrees.datacenter.table;

import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;


public class FieldNameOp {
    private String id;
    private String create;
    private String upDate;
    private String tableName;

    public static String getFieldName(String tableName, List<String> configField) {

        try {
            List<Map<String, Object>> mapList = DBUtil.query("select * from " + tableName + " limit 1");
            if (!mapList.isEmpty()) {
                Map<String, Object> firstRecord = mapList.get(0);
                Set<String> keySets = firstRecord.keySet();
                if (keySets.retainAll(configField)) {
                    for (String fieldName : keySets) {
                        return fieldName;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

    public static List<String> getConfigField(String fieldName) {
        String allName = PropertiesUtility.defaultProperties().getProperty(fieldName);
        return asList(allName.split(","));
    }

    public String getId() {
        return getFieldName(tableName, getConfigField("id"));
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCreate() {
        return getFieldName(tableName, getConfigField("create"));
    }

    public void setCreate(String create) {
        this.create = create;
    }

    public String getUpDate() {
        return getFieldName(tableName, getConfigField("update"));
    }

    public void setUpDate(String upDate) {
        this.upDate = upDate;
    }
}
