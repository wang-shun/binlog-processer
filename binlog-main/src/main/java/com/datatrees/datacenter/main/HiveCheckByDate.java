package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.HiveCompareByDate;

public class HiveCheckByDate {
    public static void main(String[] args) {
        if (args.length != 4) {
            if (args.length == 3) {
                BaseDataCompare compare = new HiveCompareByDate();
                compare.binLogCompare(args[0], "", args[1], args[2]);
            } else {
                System.out.print("please enter dataBase tableName partition partitionType");
                System.exit(1);
            }
        } else {
            BaseDataCompare compare = new HiveCompareByDate();
            compare.binLogCompare(args[0], args[1], args[2], args[3]);
        }
    }
}
