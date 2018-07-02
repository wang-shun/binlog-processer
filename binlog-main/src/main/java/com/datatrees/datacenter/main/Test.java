package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.DataCompare;
import com.datatrees.datacenter.compare.HiveCompare;
import com.datatrees.datacenter.rawdata.AvroDataReader;

public class Test {
    public static void main(String[] args) {
        //AvroDataReader reader=new AvroDataReader();
        //reader.readAllData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");
        HiveCompare dataCompare=new HiveCompare();
        String sql="select * from t_td_model_score_data where id =151277852384964608";
        dataCompare.query(sql);
    }
}
