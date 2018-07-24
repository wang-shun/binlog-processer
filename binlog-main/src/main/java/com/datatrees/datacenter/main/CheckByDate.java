package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompareByDate;

public class CheckByDate {
    public static void main(String[] args) {
        if (args.length != 4) {
            if (args.length == 3) {
                BaseDataCompare compare = new TiDBCompareByDate();
                compare.binLogCompare(args[0], "", args[1], args[2]);
            } else {
                System.out.print("please enter database tablename partition partitiontype");
                System.exit(1);
            }
        } else {
            BaseDataCompare compare = new TiDBCompareByDate();
            compare.binLogCompare(args[0], args[1], args[2], args[3]);
        }
    }
}
