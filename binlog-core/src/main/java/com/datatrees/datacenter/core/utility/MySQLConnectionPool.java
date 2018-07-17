package com.datatrees.datacenter.core.utility;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class MySQLConnectionPool extends DBConnectionPool{
    private ComboPooledDataSource cpds;
    /**
     * 在构造函数初始化的时候获取数据库连接
     */
    public MySQLConnectionPool(String dbType) {
        super(dbType);

    }
}
