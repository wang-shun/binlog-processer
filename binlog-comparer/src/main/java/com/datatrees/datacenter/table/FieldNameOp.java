package com.datatrees.datacenter.table;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;


public class FieldNameOp {
    private static Logger LOG = LoggerFactory.getLogger(FieldNameOp.class);

    public static String getFieldName(String dataBase, String tableName, List<String> configField) {
        try {
            String tableQuerySql = "SELECT table_name FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + dataBase +"'"+ " and TABLE_NAME ='" + tableName + "'";
            List<Map<String, Object>> tableExists = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", tableQuerySql);
            if (null != tableExists && tableExists.size() > 0) {
                String tableFieldSql = "select * from " + dataBase + "." + tableName + " limit 1";
                List<Map<String, Object>> mapList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, tableFieldSql);
                if (null != mapList && mapList.size() > 0) {
                    Map<String, Object> firstRecord = mapList.get(0);
                    Set<String> keySets = firstRecord.keySet();
                    Set<String> fieldSets = configField.stream().collect(Collectors.toSet());
                    if (fieldSets.retainAll(keySets)) {
                        if (fieldSets.size() > 0) {
                            return String.valueOf(fieldSets.toArray()[0]);
                        } else {
                            return null;
                        }
                    }
                }
            } else {
                LOG.info("Table " + dataBase + "." + tableName + " doesn't exist!");
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
        return null;
    }

    public static List<String> getConfigField(String fieldName) {
        String allName = PropertiesUtility.defaultProperties().getProperty(fieldName);
        return asList(allName.split(","));
    }

}
