package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.HiveClientUtility;

public class HiveCompare implements DataCompare {
    @Override
    public void binLogCompare() {

    }

    /**
     * 根绝自定义sql语句查询某条记录在hive中是否存在
     */
    public boolean query(String sql) {
        HiveClientUtility.userDefineQuery(sql);
        return true;
    }
}
