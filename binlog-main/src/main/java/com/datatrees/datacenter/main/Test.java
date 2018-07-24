package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompareByDate;
import com.datatrees.datacenter.core.utility.ConnOfC3P0Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    private static Logger LOG = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {
        /*AvroDataReader reader = new AvroDataReader();
        reader.readSrcData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");*/
        // DataCompare dataCompare = new TiDBCompare();
        //dataCompare.getSpecifiedDateTableInfo("loandb","","year=2018/month=7/day=19");

        BaseDataCompare dataCompare1 = new TiDBCompareByDate();
        dataCompare1.binLogCompare("loandb", "", "year=2018/month=7/day=20","update");
        // dataCompare.binLogCompare("1531931491-mysql-bin.000764");
        //1530494870-mysql-bin.001132.tar,1530496380-mysql-bin.000811.tar
        //LOG.info("compare finished");
      /* DataReader dataReader=new OrcDataReader();
        ((OrcDataReader) dataReader).readDestData("/orc-test/part-00008-478c4d75-3839-461b-bcf4-83009ca3bbd0.snappy.orc");*/
      /*  try {
            DBUtil.query("bill", "select * from jc_customer_ext_history limit 1");
            DBUtil.query("ecommerce", "select * from t_behavior_lable limit 1");
            DBUtil.query("collection", "select * from coll_repayment_log limit 1");
            DBUtil.query("operator", "select * from t_tel_base_info limit 1 ");
            DBUtil.query("ecommerce", "select * from t_taobao_huabei_info limit 1");
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
    }
}
