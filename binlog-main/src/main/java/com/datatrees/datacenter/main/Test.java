package com.datatrees.datacenter.main;

import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse;
import com.datatrees.datacenter.compare.HiveCompare;
import com.datatrees.datacenter.core.utility.DBConnectionPool;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.DataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;

import java.sql.SQLException;

public class Test {
    public static void main(String[] args) {
        AvroDataReader reader = new AvroDataReader();
        reader.readSrcData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");

       /*DataReader dataReader=new OrcDataReader();
        ((OrcDataReader) dataReader).readDestData("/orc-test/part-00008-478c4d75-3839-461b-bcf4-83009ca3bbd0.snappy.orc");*/

       /* try {
            DBUtil.query("select * from t_bank_mail_server_ip");
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
    }
}
