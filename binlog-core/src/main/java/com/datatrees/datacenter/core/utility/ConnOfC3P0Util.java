package com.datatrees.datacenter.core.utility;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnOfC3P0Util {
    private static Log logger = LogFactory.getLog(ConnOfC3P0Util.class);
    private static ComboPooledDataSource ds = new ComboPooledDataSource("TiDB");
    private static ComboPooledDataSource ds2 = new ComboPooledDataSource("MYSQL");

    public static ComboPooledDataSource getDs() {
        return ds;
    }

    public static Connection getConnection(String type) {
        if ("TiDB".equals(type)) {
            try {
                logger.info("ds.getDataSourceName():" + ds.getDataSourceName());
                return ds.getConnection();

            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            if ("MYSQL".equals(type)) {
                try {
                    logger.info("ds2.getDataSourceName():" + ds2.getDataSourceName());
                    return ds2.getConnection();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return null;
    }

    public static Connection getMyConnection() {
        try {
            logger.info("ds2.getDataSourceName():" + ds2.getDataSourceName());
            return ds2.getConnection();

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
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
}