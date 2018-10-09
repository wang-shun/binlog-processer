package com.datatrees.datacenter.repair.partitions;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class partitionHandler {
    private static final String FILE_SEP = File.separator;
    private static final String DATE_SEP = "=";

    public static List<String> getPartitions(Map<String, String> dateMap) {
        String year = dateMap.get("year");
        String month = dateMap.get("month");
        String day = dateMap.get("day");
        List<String> hivePartitions;
        hivePartitions = new ArrayList<>();
        hivePartitions.add(year);
        hivePartitions.add(month);
        hivePartitions.add(day);
        return hivePartitions;
    }

    public static String getHivePartition(Map<String, String> dateMap) {
        String year = dateMap.get("year");
        String month = dateMap.get("month");
        String day = dateMap.get("day");

        String hivePartition;
        hivePartition = "p_y=" +
                year +
                File.separator +
                "p_m=" +
                month +
                File.separator +
                "p_d=" +
                day;
        return hivePartition;
    }

    public static Map<String, String> getDateMap(String partition) {
        String[] date = partition.split(FILE_SEP);
        String year = date[0].split(DATE_SEP)[1];
        String month = date[1].split(DATE_SEP)[1];
        String day = date[2].split(DATE_SEP)[1];
        Map<String, String> dataMap = new HashMap<>(3);
        dataMap.put("year", year);
        dataMap.put("month", month);
        dataMap.put("day", day);
        return dataMap;
    }
}
