package com.datatrees.datacenter.core.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据库JDBC连接工具类
 *
 * @author personalc
 */
public class DBUtil {
    /**
     * the pattern of limit
     */
    private static final Pattern sLimitPattern =
            Pattern.compile("\\s*\\d+\\s*(,\\s*\\d+\\s*)?");
    private static Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    /**
     * 执行数据库插入操作
     *
     * @param valueMap  插入数据表中key为列名和value为列对应的值的Map对象
     * @param tableName 要插入的数据库的表名
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public static int insert(String dbSource, String dataBase, String tableName, Map<String, Object> valueMap) throws SQLException {

        //获取数据库插入的Map的键值对的值
        Set<String> keySet = valueMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        //要插入的字段sql，其实就是用key拼起来的
        StringBuilder columnSql = new StringBuilder();
        //要插入的字段值，其实就是？
        StringBuilder unknownMarkSql = new StringBuilder();
        Object[] bindArgs = new Object[valueMap.size()];
        int i = 0;
        while (iterator.hasNext()) {
            String key = iterator.next();
            columnSql.append(i == 0 ? "" : ",");
            columnSql.append(key);

            unknownMarkSql.append(i == 0 ? "" : ",");
            unknownMarkSql.append("?");
            bindArgs[i] = valueMap.get(key);
            i++;
        }
        //开始拼插入的sql语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append("`" + dataBase + "`." + tableName);
        sql.append(" (");
        sql.append(columnSql);
        sql.append(" )  VALUES (");
        sql.append(unknownMarkSql);
        sql.append(" )");
        return executeUpdate(dbSource, dataBase, sql.toString(), bindArgs);
    }

    public static int upsert(String dbSource, String dataBase, String tableName, Map<String, Object> valueMap) throws SQLException {

        //获取数据库插入的Map的键值对的值
        Set<String> keySet = valueMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        //要插入的字段sql，其实就是用key拼起来的
        StringBuilder columnSql = new StringBuilder();
        //要插入的字段值，其实就是？
        StringBuilder unknownMarkSql = new StringBuilder();
        StringBuilder upsertSql = new StringBuilder();
        Object[] bindArgs = new Object[valueMap.size()];
        int i = 0;
        while (iterator.hasNext()) {
            String key = iterator.next();
            columnSql.append(i == 0 ? "" : ",");
            columnSql.append(key);

            unknownMarkSql.append(i == 0 ? "" : ",");
            unknownMarkSql.append("?");
            bindArgs[i] = valueMap.get(key);

            upsertSql.append(i == 0 ? "" : ",");
            upsertSql.append(key + "=values(" + key + ")");
            i++;
        }
        //开始拼插入的sql语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append("`" + dataBase + "`." + tableName);
        sql.append(" (");
        sql.append(columnSql);
        sql.append(" )  VALUES (");
        sql.append(unknownMarkSql);
        sql.append(" )");
        sql.append(" on duplicate key update ");
        sql.append(upsertSql);
        return executeUpdate(dbSource, dataBase, sql.toString(), bindArgs);
    }


    /**
     * 执行数据库插入操作
     *
     * @param datas     插入数据表中key为列名和value为列对应的值的Map对象的List集合
     * @param tableName 要插入的数据库的表名
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public static int insertAll(String dbSource, String dataBase, String tableName, List<Map<String, Object>> datas)
            throws SQLException {
        //影响的行数
        int affectRowCount = -1;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        if (datas != null && datas.size() > 0) {
            try {
                //从数据库连接池中获取数据库连接
                connection = ConnOfC3P0Util.getInstance().getConnection(dbSource);
                Map<String, Object> valueMap = datas.get(0);
                //获取数据库插入的Map的键值对的值
                Set<String> keySet = valueMap.keySet();
                Iterator<String> iterator = keySet.iterator();
                //要插入的字段sql，其实就是用key拼起来的
                StringBuilder columnSql = new StringBuilder();
                //要插入的字段值，其实就是?
                StringBuilder unknownMarkSql = new StringBuilder();
                Object[] keys = new Object[valueMap.size()];
                int i = 0;
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    keys[i] = key;
                    columnSql.append(i == 0 ? "" : ",");
                    columnSql.append(key);

                    unknownMarkSql.append(i == 0 ? "" : ",");
                    unknownMarkSql.append("?");
                    i++;
                }
                //开始拼插入的sql语句
                StringBuilder sql = new StringBuilder();
                sql.append("INSERT INTO ");
                sql.append("`" + dataBase + "`." + tableName);
                sql.append(" (");
                sql.append(columnSql);
                sql.append(" )  VALUES (");
                sql.append(unknownMarkSql);
                sql.append(" )");

                //执行SQL预编译
                preparedStatement = connection.prepareStatement(sql.toString());
                //设置不自动提交，以便于在出现异常的时候数据库回滚
                connection.setAutoCommit(false);
                System.out.println(sql.toString());
                for (int j = 0; j < datas.size(); j++) {
                    for (int k = 0; k < keys.length; k++) {
                        preparedStatement.setObject(k + 1, datas.get(j).get(keys[k]));
                    }
                    preparedStatement.addBatch();
                }
                int[] arr = preparedStatement.executeBatch();
                connection.commit();
                affectRowCount = arr.length;
                LOG.info("成功了插入了" + affectRowCount + "行");
                System.out.println();
            } catch (Exception e) {
                LOG.error("error to insert all", e);
                if (connection != null) {
                    connection.setAutoCommit(false);
                    connection.rollback();
                }
                throw e;
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
        return affectRowCount;
    }

    /**
     * 执行数据库插入操作
     *
     * @param datas     插入数据表中key为列名和value为列对应的值的Map对象的List集合
     * @param tableName 要插入的数据库的表名
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public static int upsertAll(String dbSource, String dataBase, String tableName, List<Map<String, Object>> datas)
            throws SQLException {
        //影响的行数
        int affectRowCount = -1;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        if (datas != null && datas.size() > 0) {
            try {
                //从数据库连接池中获取数据库连接
                connection = ConnOfC3P0Util.getInstance().getConnection(dbSource);
                Map<String, Object> valueMap = datas.get(0);
                //获取数据库插入的Map的键值对的值
                Set<String> keySet = valueMap.keySet();
                Iterator<String> iterator = keySet.iterator();
                //要插入的字段sql，其实就是用key拼起来的
                StringBuilder columnSql = new StringBuilder();
                //要插入的字段值，其实就是?
                StringBuilder unknownMarkSql = new StringBuilder();
                //存在则更新
                StringBuilder upsertSql = new StringBuilder();
                Object[] keys = new Object[valueMap.size()];
                int i = 0;
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    keys[i] = key;
                    columnSql.append(i == 0 ? "" : ",");
                    columnSql.append(key);

                    unknownMarkSql.append(i == 0 ? "" : ",");
                    unknownMarkSql.append("?");

                    upsertSql.append(i == 0 ? "" : ",");
                    upsertSql.append(key + "=values(" + key + ")");

                    i++;

                }
                //开始拼插入的sql语句
                StringBuilder sql = new StringBuilder();
                sql.append("INSERT INTO ");
                sql.append("`" + dataBase + "`." + tableName);
                sql.append(" (");
                sql.append(columnSql);
                sql.append(" )  VALUES (");
                sql.append(unknownMarkSql);
                sql.append(" )");
                sql.append(" on duplicate key update ");
                sql.append(upsertSql);

                //执行SQL预编译
                preparedStatement = connection.prepareStatement(sql.toString());
                //设置不自动提交，以便于在出现异常的时候数据库回滚
                connection.setAutoCommit(false);
                System.out.println(sql.toString());
                for (int j = 0; j < datas.size(); j++) {
                    for (int k = 0; k < keys.length; k++) {
                        preparedStatement.setObject(k + 1, datas.get(j).get(keys[k]));
                    }
                    preparedStatement.addBatch();
                }
                int[] arr = preparedStatement.executeBatch();
                connection.commit();
                affectRowCount = arr.length;
                LOG.info("成功了插入了" + affectRowCount + "行");
                System.out.println();
            } catch (Exception e) {
                LOG.error("error to insert all", e);
                if (connection != null) {
                    connection.setAutoCommit(false);
                    connection.rollback();
                }
                throw e;
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
        return affectRowCount;
    }

    /**
     * 执行更新操作
     *
     * @param tableName 表名
     * @param valueMap  要更改的值
     * @param whereMap  条件
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public static int update(String dbSource, String dataBase, String tableName, Map<String, Object> valueMap,
                             Map<String, Object> whereMap) throws SQLException {
        //获取数据库插入的Map的键值对的值
        Set<String> keySet = valueMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        //开始拼插入的sql语句
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ");
        sql.append("`" + dataBase + "`." + tableName);
        sql.append(" SET ");

        //要更改的的字段sql，其实就是用key拼起来的
        StringBuilder columnSql = new StringBuilder();
        int i = 0;
        List<Object> objects = new ArrayList<>();
        while (iterator.hasNext()) {
            String key = iterator.next();
            columnSql.append(i == 0 ? "" : ",");
            columnSql.append(key).append(" = ? ");
            objects.add(valueMap.get(key));
            i++;
        }
        sql.append(columnSql);

        //更新的条件:要更改的的字段sql，其实就是用key拼起来的
        StringBuilder whereSql = new StringBuilder();
        int j = 0;
        if (whereMap != null && whereMap.size() > 0) {
            whereSql.append(" WHERE ");
            iterator = whereMap.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereSql.append(j == 0 ? "" : " AND ");
                whereSql.append(key).append(" = ? ");
                objects.add(whereMap.get(key));
                j++;
            }
            sql.append(whereSql);
        }
        return executeUpdate(dbSource, dataBase, sql.toString(), objects.toArray());
    }

    /**
     * 执行删除操作
     *
     * @param tableName 要删除的表名
     * @param whereMap  删除的条件
     * @return 影响的行数
     * @throws SQLException SQL执行异常
     */
    public static int delete(String dbSource, String dataBase, String tableName, Map<String, Object> whereMap) throws SQLException {
        //准备删除的sql语句
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ");
        sql.append("`" + dataBase + "`." + tableName);

        //更新的条件:要更改的的字段sql，其实就是用key拼起来的
        StringBuilder whereSql = new StringBuilder();
        Object[] bindArgs = null;
        if (whereMap != null && whereMap.size() > 0) {
            bindArgs = new Object[whereMap.size()];
            whereSql.append(" WHERE ");
            //获取数据库插入的Map的键值对的值
            Set<String> keySet = whereMap.keySet();
            Iterator<String> iterator = keySet.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereSql.append(i == 0 ? "" : " AND ");
                whereSql.append(key + " = ? ");
                bindArgs[i] = whereMap.get(key);
                i++;
            }
            sql.append(whereSql);
        }
        return executeUpdate(dbSource, dataBase, sql.toString(), bindArgs);
    }

    /**
     * 可以执行新增，修改，删除
     *
     * @param sql      sql语句
     * @param bindArgs 绑定参数
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    private static int executeUpdate(String dbSource, String dataBase, String sql, Object[] bindArgs) throws SQLException {
        //影响的行数
        int affectRowCount;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            //从数据库连接池中获取数据库连接
            connection = ConnOfC3P0Util.getInstance().getConnection(dbSource);
            //执行SQL预编译
            preparedStatement = connection.prepareStatement(sql);
            //设置不自动提交，以便于在出现异常的时候数据库回滚
            connection.setAutoCommit(false);
            LOG.info(getExecSQL(sql, bindArgs));
            if (bindArgs != null) {
                //绑定参数设置sql占位符中的值
                for (int i = 0; i < bindArgs.length; i++) {
                    preparedStatement.setObject(i + 1, bindArgs[i]);
                }
            }
            //执行sql
            affectRowCount = preparedStatement.executeUpdate();
            connection.commit();
            String operate;
            if (sql.toUpperCase().contains("DELETE FROM")) {
                operate = "删除";
            } else if (sql.toUpperCase().contains("INSERT INTO")) {
                operate = "新增";
            } else {
                operate = "修改";
            }
            LOG.info("成功" + operate + "了" + affectRowCount + "行");
            System.out.println();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
            throw e;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return affectRowCount;
    }


    public static List<Map<String, Object>> query(String dbSource, String dataBase, String sql) throws SQLException {
        return executeQuery(dbSource, dataBase, sql, null);
    }


    public static List<Map<String, Object>> query(String dbSource, String dataBase, String tableName,
                                                  Map<String, Object> whereMap) throws Exception {
        StringBuilder whereClause = new StringBuilder();
        Object[] whereArgs = null;
        if (whereMap != null && whereMap.size() > 0) {
            Iterator<String> iterator = whereMap.keySet().iterator();
            whereArgs = new Object[whereMap.size()];
            int i = 0;
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereClause.append(i == 0 ? "" : " AND ");
                whereClause.append(key).append(" = ? ");
                whereArgs[i] = whereMap.get(key);
                i++;
            }
        }
        return query(dbSource, dataBase, tableName, false, null, whereClause.toString(), whereArgs, null, null, null, null);
    }


    public static List<Map<String, Object>> query(String dbSource, String dataBase, String tableName,
                                                  String whereClause,
                                                  String[] whereArgs) throws SQLException {
        return query(dbSource, dataBase, tableName, false, null, whereClause, whereArgs, null, null, null, null);
    }


    public static List<Map<String, Object>> query(String dbSource, String dataBase, String tableName,
                                                  boolean distinct,
                                                  String[] columns,
                                                  String selection,
                                                  Object[] selectionArgs,
                                                  String groupBy,
                                                  String having,
                                                  String orderBy,
                                                  String limit) throws SQLException {
        String sql = buildQueryString(distinct, "`" + dataBase + "`." + tableName, columns, selection, groupBy, having, orderBy,
                limit);
        return executeQuery(dbSource, dataBase, sql, selectionArgs);

    }

    private static List<Map<String, Object>> executeQuery(String dbSource, String dataBase, String sql, Object[] bindArgs)
            throws SQLException {
        List<Map<String, Object>> datas = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //获取数据库连接池中的连接
            connection = ConnOfC3P0Util.getInstance().getConnection(dbSource);
            if (null != connection) {
                preparedStatement = connection.prepareStatement(sql);
                if (bindArgs != null) {
                    //设置sql占位符中的值
                    for (int i = 0; i < bindArgs.length; i++) {
                        preparedStatement.setObject(i + 1, bindArgs[i]);
                    }
                }
                LOG.info(getExecSQL(sql, bindArgs));
                //执行sql语句，获取结果集
                resultSet = preparedStatement.executeQuery();
                datas = getDatas(resultSet);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return datas;
    }

    /**
     * 将结果集对象封装成List<Map<String, Object>> 对象
     *
     * @param resultSet 结果
     * @return 结果的封装
     */
    private static List<Map<String, Object>> getDatas(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> datas = new ArrayList<>();
        //获取结果集的数据结构对象
        if (null != resultSet) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, Object> rowMap = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    rowMap.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                datas.add(rowMap);
            }
            LOG.info("成功查询到了" + datas.size() + "行数据");
            for (int i = 0; i < datas.size(); i++) {
                Map<String, Object> map = datas.get(i);
                System.out.println("第" + (i + 1) + "行：" + map);
            }
        }
        return datas;
    }

    /**
     * Build an SQL query string from the given clauses.
     *
     * @param distinct true if you want each row to be unique, false otherwise.
     * @param tables   The table names to compile the query against.
     * @param columns  A list of which columns to return. Passing null will
     *                 return all columns, which is discouraged to prevent reading
     *                 data from storage that isn't going to be used.
     * @param where    A filter declaring which rows to return, formatted as an SQL
     *                 WHERE clause (excluding the WHERE itself). Passing null will
     *                 return all rows for the given URL.
     * @param groupBy  A filter declaring how to group rows, formatted as an SQL
     *                 GROUP BY clause (excluding the GROUP BY itself). Passing null
     *                 will cause the rows to not be grouped.
     * @param having   A filter declare which row groups to include in the cursor,
     *                 if row grouping is being used, formatted as an SQL HAVING
     *                 clause (excluding the HAVING itself). Passing null will cause
     *                 all row groups to be included, and is required when row
     *                 grouping is not being used.
     * @param orderBy  How to order the rows, formatted as an SQL ORDER BY clause
     *                 (excluding the ORDER BY itself). Passing null will use the
     *                 default sort order, which may be unordered.
     * @param limit    Limits the number of rows returned by the query,
     *                 formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return the SQL query string
     */
    private static String buildQueryString(
            boolean distinct, String tables, String[] columns, String where,
            String groupBy, String having, String orderBy, String limit) {
        if (isEmpty(groupBy) && !isEmpty(having)) {
            throw new IllegalArgumentException(
                    "HAVING clauses are only permitted when using a groupBy clause");
        }
        if (!isEmpty(limit) && !sLimitPattern.matcher(limit).matches()) {
            throw new IllegalArgumentException("invalid LIMIT clauses:" + limit);
        }

        StringBuilder query = new StringBuilder(120);

        query.append("SELECT ");
        if (distinct) {
            query.append("DISTINCT ");
        }
        if (columns != null && columns.length != 0) {
            appendColumns(query, columns);
        } else {
            query.append(" * ");
        }
        query.append("FROM ");
        query.append(tables);
        appendClause(query, " WHERE ", where);
        appendClause(query, " GROUP BY ", groupBy);
        appendClause(query, " HAVING ", having);
        appendClause(query, " ORDER BY ", orderBy);
        appendClause(query, " LIMIT ", limit);
        return query.toString();
    }

    /**
     * Add the names that are non-null in columns to s, separating
     * them with commas.
     */
    private static void appendColumns(StringBuilder s, String[] columns) {
        int n = columns.length;

        for (int i = 0; i < n; i++) {
            String column = columns[i];

            if (column != null) {
                if (i > 0) {
                    s.append(", ");
                }
                s.append(column);
            }
        }
        s.append(' ');
    }

    /**
     * addClause
     *
     * @param s      the add StringBuilder
     * @param name   clauseName
     * @param clause clauseSelection
     */
    private static void appendClause(StringBuilder s, String name, String clause) {
        if (!isEmpty(clause)) {
            s.append(name);
            s.append(clause);
        }
    }

    /**
     * Returns true if the string is null or 0-length.
     *
     * @param str the string to be examined
     * @return true if str is null or zero length
     */
    private static boolean isEmpty(CharSequence str) {
        return str == null || str.length() == 0;
    }

    /**
     * After the execution of the complete SQL statement, not necessarily the actual implementation of the SQL statement
     *
     * @param sql      SQL statement
     * @param bindArgs Binding parameters
     * @return Replace? SQL statement executed after the
     */
    private static String getExecSQL(String sql, Object[] bindArgs) {
        StringBuilder sb = new StringBuilder(sql);
        if (bindArgs != null && bindArgs.length > 0) {
            int index = 0;
            for (int i = 0; i < bindArgs.length; i++) {
                index = sb.indexOf("?", index);
                sb.replace(index, index + 1, String.valueOf(bindArgs[i]));
            }
        }
        return sb.toString();
    }
}