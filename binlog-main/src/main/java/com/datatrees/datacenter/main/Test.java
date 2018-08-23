package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompare;
import com.datatrees.datacenter.compare.TiDBCompareByDate;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.BaseDataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Test {
    private static Logger LOG = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {

        /*AvroDataReader reader = new AvroDataReader();
        reader.readSrcData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");*/
           /* BaseDataCompare dataCompare = new TiDBCompare();
            dataCompare.binLogCompare("1534491345-mysql-bin.001317","create");*/
        //AvroDataReader reader=new AvroDataReader();
        //reader.readSrcData("hdfs://cloudera3/data/warehouse/create/basisdataoperator/operator/t_tel_call_sheet/year=2017/month=10/day=18/1532831715-mysql-bin.000735.avro");

        BaseDataCompare dataCompare1 = new TiDBCompareByDate();
        dataCompare1.binLogCompare("ecommerce", "", "year=2018/month=8/day=22", "update");
        // dataCompare.binLogCompare("1531931491-mysql-bin.000764");
        //1530494870-mysql-bin.001132.tar,1530496380-mysql-bin.000811.tar
        //LOG.info("compare finished");
       /* BaseDataReader dataReader = new OrcDataReader();
        ((OrcDataReader) dataReader).readDestData("coll_account_age/year=2018/month=5/day=18/delta_0000009_0000009_0000/bucket_00000");*/
        /*BaseDataReader dataReader = new OrcDataReader();
        ((OrcDataReader) dataReader).readDestData("/orc-test/part-00008-478c4d75-3839-461b-bcf4-83009ca3bbd0.snappy.orc");*/

        //AvroDataReader.readAllDataFromAvro("hdfs://cloudera3/data/warehouse/update/antifraud/antifraud/atf_commerce_record/year=2018/month=8/day=8/1533712536-mysql-bin.000326.avro");
        /*try {
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
