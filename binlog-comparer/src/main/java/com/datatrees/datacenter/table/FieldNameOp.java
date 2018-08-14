package com.datatrees.datacenter.table;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;


public class FieldNameOp {
    private static Logger LOG = LoggerFactory.getLogger(FieldNameOp.class);

    public static String getFieldName(Set<String> allFieldName, List<String> configField) {
        Set<String> fieldSets = configField.stream().collect(Collectors.toSet());
        if (allFieldName.retainAll(fieldSets)) {
            if (allFieldName.size() > 0 && allFieldName.size() > 0) {
                return String.valueOf(fieldSets.toArray()[0]);
            } else {
                return null;
            }
        }
        return null;
    }

    public static List<String> getConfigField(String fieldName) {
        String allName = PropertiesUtility.defaultProperties().getProperty(fieldName);
        return new ArrayList<>(asList(allName.split(",")));
    }

    public static Set<String> getAllFieldName(String dataBase, String tableName) {
        try {
            String tableQuerySql = "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + dataBase + "'" + " and TABLE_NAME ='" + tableName + "'";
            List<Map<String, Object>> tableExists = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", tableQuerySql);
            if (null != tableExists && tableExists.size() > 0) {
                String tableFieldSql = "select * from " + "`" + dataBase + "`." + tableName + " limit 1";
                List<Map<String, Object>> mapList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, tableFieldSql);
                if (null != mapList && mapList.size() > 0) {
                    Map<String, Object> firstRecord = mapList.get(0);
                    Set<String> keySets = firstRecord.keySet();
                    return keySets;
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
