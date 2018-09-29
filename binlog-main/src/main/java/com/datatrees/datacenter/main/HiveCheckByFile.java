package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.HiveCompareByFile;

public class HiveCheckByFile {
    public static void main(String[] args) {
        if (args.length == 2) {
            BaseDataCompare compare = new HiveCompareByFile();
            compare.binLogCompare(args[0], args[1]);
        } else {
            System.out.print("please enter fileName partitionType");
            System.exit(1);
        }
    }
}
