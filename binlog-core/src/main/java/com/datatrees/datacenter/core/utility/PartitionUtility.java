package com.datatrees.datacenter.core.utility;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionUtility {
    private static final String FILE_SEP = File.separator;
    private static final String DATE_SEP = "=";
    private static final String YEAR = "year";
    private static final String MONTH = "month";
    private static final String DAY = "day";
    private static final String YEAR_PREFIX = "p_y=";
    private static final String MONTH_PREFIX = "p_m=";
    private static final String DAY_PREFIX = "p_d=";

    public static List<String> getPartitions(Map<String, String> dateMap) {
        String year = dateMap.get(YEAR);
        String month = dateMap.get(MONTH);
        String day = dateMap.get(DAY);
        List<String> hivePartitions;
        hivePartitions = new ArrayList<>();
        hivePartitions.add(year);
        hivePartitions.add(month);
        hivePartitions.add(day);
        return hivePartitions;
    }

    public static String getHivePartition(Map<String, String> dateMap) {
        String year = dateMap.get(YEAR);
        String month = dateMap.get(MONTH);
        String day = dateMap.get(DAY);

        String hivePartition;
        hivePartition = YEAR_PREFIX +
                year +
                File.separator +
                MONTH_PREFIX +
                month +
                File.separator +
                DAY_PREFIX +
                day;
        return hivePartition;
    }

    public static Map<String, String> getDateMap(String partition) {
        String[] date = partition.split(FILE_SEP);
        String year = date[0].split(DATE_SEP)[1];
        String month = date[1].split(DATE_SEP)[1];
        String day = date[2].split(DATE_SEP)[1];
        Map<String, String> dataMap = new HashMap<>(3);
        dataMap.put(YEAR, year);
        dataMap.put(MONTH, month);
        dataMap.put(DAY, day);
        return dataMap;
    }
}
