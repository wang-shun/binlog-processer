package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.utility.DBConnectionPool;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.transfer.bean.TableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SendCheck {
    public static void main(String[] args) {
        getSendFailuredRecord();
    }

    private static List<Map<String, Object>> getSendFailuredRecord() {
        Map<String, String> selectField = new HashMap<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> datas = new ArrayList<>();
        try {
            connection = DBConnectionPool.getInstance().getConnection();
            String tableField = TableInfo.BINLOG_TRANS_TABLE + ".id";
            String tableTran = TableInfo.BINLOG_TRANS_TABLE;
            String tableProc = TableInfo.BINLOG_PROC_TABLE;
            String sql = "select r.id from t_binlog_record as r left join t_binlog_process as p on r.db_instance=p.db_instance and r.file_name=p.file_name where p.id is null";
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            datas = DBUtil.getDatas(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(datas.size());
        return datas;
    }
}
