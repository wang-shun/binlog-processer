package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompare;

public class CheckByFile {
    public static void main(String[] args) {
        if (args.length == 2) {
            BaseDataCompare compare = new TiDBCompare();
            compare.binLogCompare(args[0], args[1]);
        } else {
            System.out.print("please enter fileName partitionType");
            System.exit(1);
        }
    }
}
