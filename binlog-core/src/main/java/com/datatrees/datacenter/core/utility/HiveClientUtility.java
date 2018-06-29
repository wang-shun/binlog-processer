package com.datatrees.datacenter.core.utility;

import java.sql.*;

public class HiveClientUtility {

    private static String driverName ="org.apache.hive.jdbc.HiveDriver";

    //填写hive的IP，之前在配置文件中配置的IP

    private static String Url="jdbc:hive2://cloudera2:10000/tongdun";
    private static Connection conn;

    private static PreparedStatement ps;

    private static ResultSet rs;

    private static String HDFSUser="hdfs";

    private static String HDFSPassword="";

    //创建连接

    public static Connection getConnnection(){

        try {
            Class.forName(driverName);
            //此处的用户名一定是有权限操作HDFS的用户，否则程序会提示"permission deny"异常
            conn = DriverManager.getConnection(Url,HDFSUser,HDFSPassword);
        } catch(ClassNotFoundException e)  {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static PreparedStatement prepare(Connection conn, String sql) {

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }

    public static void getAll(String tableName) {

        conn=getConnnection();
        String sql="select * from "+tableName+" limit 100";
        System.out.println(sql);
        try {
            ps=prepare(conn, sql);
            rs=ps.executeQuery();
            int columns=rs.getMetaData().getColumnCount();
            while(rs.next()) {
                for(int i=1;i<=columns;i++) {
                    System.out.print(rs.getString(i));
                    System.out.print("\t\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void userDefineQuery(String sqlStr){
        conn=getConnnection();
        ps=prepare(conn,sqlStr);
        try {
            rs=ps.executeQuery();
            int columns=rs.getMetaData().getColumnCount();
            while(rs.next()) {
                for(int i=1;i<=columns;i++) {
                    System.out.print(rs.getString(i));
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tablename="t_td_model_score_data";
        getAll(tablename);
    }

}
