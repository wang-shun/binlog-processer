package com.datatrees.datacenter.core.utility;

public class DataBaseNameHandler {
    public static String dataBaseNameChange(String dataBaseName){
        if("bankbill".equals(dataBaseName)) {
            return "bill";
        }else{
            return dataBaseName;
        }
    }
}
