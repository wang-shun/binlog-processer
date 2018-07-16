package com.datatrees.datacenter.table;

import com.datatrees.datacenter.compare.TiDBCompare;
import com.datatrees.datacenter.core.utility.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;


public class FieldNameOp {
    private static Logger LOG = LoggerFactory.getLogger(FieldNameOp.class);

    // TODO: 2018/7/11 修改返回记录，在方法调用处解析id、lastUpatetime等
    public static String getFieldName(String dataBase, String tableName, List<String> configField) {
        try {
            List<Map<String, Object>> mapList = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.TIDB.toString()), dataBase, "select * from " + tableName + " limit 1");
            if (null != mapList) {
                Map<String, Object> firstRecord = mapList.get(0);
                Set<String> keySets = firstRecord.keySet();
                if (keySets.retainAll(configField)) {
                    for (String fieldName : keySets) {
                        return fieldName;
                    }
                }
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
