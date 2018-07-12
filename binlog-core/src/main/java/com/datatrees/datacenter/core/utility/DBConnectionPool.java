package com.datatrees.datacenter.core.utility;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 数据库连接对象
 *
 * @author personalc
 */

public class DBConnectionPool {

    private static volatile DBConnectionPool dbConnection;
    private ComboPooledDataSource cpds;
    private DBInfo dbInfo;

    /**
     * 在构造函数初始化的时候获取数据库连接
     */
    public DBConnectionPool(DBInfo dbInfo) {
        this.dbInfo = dbInfo;
        try {
            //数据库连接池对象
            cpds = new ComboPooledDataSource();
            //设置数据库连接驱动
            cpds.setDriverClass(dbInfo.driver);
            //设置数据库连接地址
            cpds.setJdbcUrl(dbInfo.url);
            //设置数据库连接用户名
            cpds.setUser(dbInfo.username);
            //设置数据库连接密码
            cpds.setPassword(dbInfo.password);
            //初始化时创建的连接数,应在minPoolSize与maxPoolSize之间取值.默认为3
            cpds.setInitialPoolSize(3);
            //连接池中保留的最大连接数据.默认为15
            cpds.setMaxPoolSize(10);
            //当连接池中的连接用完时，C3PO一次性创建新的连接数目;
            cpds.setAcquireIncrement(1);
            //隔多少秒检查所有连接池中的空闲连接,默认为0表示不检查;
            cpds.setIdleConnectionTestPeriod(60);
            //最大空闲时间,超过空闲时间的连接将被丢弃.为0或负数据则永不丢弃.默认为0;
            cpds.setMaxIdleTime(3000);
            //因性能消耗大请只在需要的时候使用它。如果设为true那么在每个connection提交的
            //时候都将校验其有效性。建议使用idleConnectionTestPeriod或automaticTestTable
            //等方法来提升连接测试的性能。Default: false
            cpds.setTestConnectionOnCheckout(true);
            //如果设为true那么在取得连接的同时将校验连接的有效性。Default: false
            cpds.setTestConnectionOnCheckin(true);
            //定义在从数据库获取新的连接失败后重复尝试获取的次数，默认为30;
            cpds.setAcquireRetryAttempts(30);
            //两次连接中间隔时间默认为1000毫秒
            cpds.setAcquireRetryDelay(1000);
            // 获取连接失败将会引起所有等待获取连接的线程异常,
            // 但是数据源仍有效的保留, 并在下次调用getConnection() 的时候继续尝试获取连接.如果设为true,
            // 那么尝试获取连接失败后该数据源将申明已经断开并永久关闭.默认为false
            cpds.setBreakAfterAcquireFailure(true);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库连接对象，单例
     */

    public DBConnectionPool getInstance() {
        if (dbConnection == null) {
            synchronized (DBConnectionPool.class) {
                if (dbConnection == null) {
                    dbConnection = new DBConnectionPool(dbInfo);
                }
            }
        }
        else {
            String lastUrl=dbConnection.dbInfo.getUrl();
            System.out.println(lastUrl);
            if(!lastUrl.equalsIgnoreCase(dbInfo.url)) {
                dbConnection.finalize();
                dbConnection=new DBConnectionPool(dbInfo);
            }
        }
        return dbConnection;
    }

    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     */

    public final synchronized Connection getConnection(String dataBase) throws SQLException {
        String url = cpds.getJdbcUrl();
        String subUrl = url.substring(url.lastIndexOf("/") + 1, url.lastIndexOf("?"));
        cpds.setJdbcUrl(url.replace(subUrl, dataBase));
        return cpds.getConnection();
    }

    /**
     * finalize()方法是在垃圾收集器删除对象之前对这个对象调用的。
     */

    @Override
    protected void finalize()  {
        try {
            DataSources.destroy(cpds);
            super.finalize();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    static class DBInfo {
        private String driver;
        private String url;
        private String username;
        private String password;

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
