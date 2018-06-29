package com.datatrees.datacenter.main;

import com.datatrees.datacenter.rawdata.AvroDataReader;

public class Test {
    public static void main(String[] args) {
        AvroDataReader reader=new AvroDataReader();
        reader.readBinLogData("/data/warehouse/create/third-server/tongdun/t_td_risk_user_summary/");
    }
}
