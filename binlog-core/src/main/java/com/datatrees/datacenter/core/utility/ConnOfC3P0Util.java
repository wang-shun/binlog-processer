package com.datatrees.datacenter.core.utility;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class ConnOfC3P0Util {
    private static Log logger = LogFactory.getLog(ConnOfC3P0Util.class);
    private static ComboPooledDataSource ds = new ComboPooledDataSource();
    private static ComboPooledDataSource ds2 = new ComboPooledDataSource(DBServer.DBServerType.TIDB.toString());
    private static volatile ConnOfC3P0Util dbConnection;

    public final synchronized Connection getConnection(String type) {
        try {
            if (DBServer.DBServerType.TIDB.toString().equals(type)) {
                return ds2.getConnection();
            } else {
                return ds.getConnection();
            }
        } catch (SQLException e) {
            logger.info("can't get connection of server type : " + type, e);
        }
        return null;
    }

    public static ConnOfC3P0Util getInstance() {
        if (dbConnection == null) {
            synchronized (ConnOfC3P0Util.class) {
                if (dbConnection == null) {
                    dbConnection = new ConnOfC3P0Util();
                }
            }
        }
        return dbConnection;
    }

    public static void free(ResultSet rs, Statement st, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * finalize()方法是在垃圾收集器删除对象之前对这个对象调用的。
     */
    protected void closeComboPool(ComboPooledDataSource cpds) {
        try {
            DataSources.destroy(cpds);
            super.finalize();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}