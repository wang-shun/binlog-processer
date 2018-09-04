package com.datatrees.datacenter.utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StringBuilderUtil {
    /**
     * 组装where语句
     *
     * @param whereMap
     * @return
     */
    public static StringBuilder getStringBuilder(Map<String, String> whereMap) {
        whereMap.values().remove("");
        List<Map.Entry<String, String>> map2List;
        map2List = new ArrayList<>(whereMap.entrySet());
        StringBuilder whereExpress = new StringBuilder();
        if (whereMap.size() > 0) {
            whereExpress.append(" where ");
            for (int i = 0, length = map2List.size(); i < length; i++) {
                Map.Entry<String, String> express = map2List.get(i);
                whereExpress
                        .append(express.getKey())
                        .append("=")
                        .append("'")
                        .append(express.getValue())
                        .append("'")
                        .append(" ");
                if (i < map2List.size() - 1) {
                    whereExpress
                            .append(" ")
                            .append("and")
                            .append(" ");
                }
            }
        }
        return whereExpress;
    }
}
