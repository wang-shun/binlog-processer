package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompare;
import com.datatrees.datacenter.compare.TiDBCompareByDate;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.BaseDataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;


import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.TiDBDataRepair;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import javafx.beans.binding.ObjectExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tukaani.xz.check.Check;

import java.util.*;

public class Test {
    private static Logger LOG = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {
        // TODO: 2018/8/27 新版本发布前需要检查配置文件是否需要更新
        /*AvroDataReader reader = new AvroDataReader();
        reader.readSrcData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");*/

        BaseDataCompare dataCompare = new TiDBCompare();
        dataCompare.binLogCompare("1536058717-mysql-bin.000997", "update");

       /* AvroDataReader reader=new AvroDataReader();
        reader.readSrcData("hdfs://cloudera3/data/warehouse/update/gongfudai/loandb/t_user_blacklist/year=2018/month=7/day=5/1536058717-mysql-bin.000997.avro");*/

        //dataCompare1.binLogCompare("ecommerce", "t_taobao_address", "year=2018/month=8/day=20", "update");

       /* BaseDataCompare dataCompare1 = new TiDBCompareByDate();
        dataCompare1.binLogCompare("test_db", "zz", "year=2018/month=8/day=20", "update");
*/
        //dataCompare.binLogCompare("1531931491-mysql-bin.000764");
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

        //tiDBDataRepair.repairByTime("test_db", "zz", "year=2018/month=8/day=20", "update");

        /*TiDBDataRepair tiDBDataRepair = new TiDBDataRepair();
        tiDBDataRepair.repairByTime("test_db", "zz", "year=2018/month=8/day=20", "update");*/
        /*List<String> idList = new ArrayList<>();
        idList.add("216486752306237456");
        idList.add(" 216486752306237457");
        idList.add("216486752297844822");
        idList.add("216486752306237458");
        AvroDataReader.filterDataByIdList("hdfs://cloudera3/data/warehouse/update/dataplatform/basisdata/wy_basic_shopping_sheet/year=2018/month=8/day=21", "basisdata", "wy_basic_shopping_sheet", idList);*/

        //repair test
       /* CheckResult checkResult = new CheckResult();
        String dbInstance = "gongfudai";
        String dataBase = "test_db";
        String partition = "year=2018/month=8/day=20";
        String partitionType = "update";
        String tableName = "zz";
        checkResult.setDbInstance(dbInstance);
        checkResult.setDataBase(dataBase);
        checkResult.setFilePartition(partition);
        checkResult.setPartitionType(partitionType);
        checkResult.setTableName(tableName);
        Map<String, Object> whereMap = new HashMap<>();
        whereMap.put(CheckTable.DB_INSTANCE, dbInstance);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.values().remove("");
        try {
            List<Map<String, Object>> list = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, CheckTable.BINLOG_CHECK_DATE_TABLE, whereMap);
            TiDBDataRepair repair = new TiDBDataRepair();
            if (list != null && list.size() > 0) {
                for (int i = 0; i < list.size(); i++) {
                    String table = (String) list.get(i).get(CheckTable.TABLE_NAME);
                    checkResult.setTableName(table);
                    repair.repairByIdList(checkResult, CheckTable.BINLOG_CHECK_DATE_TABLE);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        TiDBDataRepair repair = new TiDBDataRepair();
        repair.repairByIdList(checkResult, CheckTable.BINLOG_CHECK_DATE_TABLE);*/

       /* Map<String, List<Set<Map.Entry<String, Object>>>> dataRecord = AvroDataReader.readAvroDataById(checkResult, CheckTable.BINLOG_CHECK_DATE_TABLE);
        List<Set<Map.Entry<String, Object>>> dataMap = dataRecord.get(OperateType.Create.toString());
         System.out.println(dataMap.size());
        Set<Map.Entry<String, Object>> sets = dataMap.get(0);
        System.out.println(sets.size());*/

        // TODO: 2018/9/3 新增修复结果保存到数据库


    }
}
