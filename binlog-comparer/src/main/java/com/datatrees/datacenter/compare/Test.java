package com.datatrees.datacenter.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {
    private static Logger LOG = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {
        Map<String, Long> map1 = new HashMap<String, Long>();
        map1.put("A", 1L);
        map1.put("C", 2L);
        map1.put("E", 3L);
        map1.put("E", 4L);
        map1.put("H", 5L);
        System.out.println(map1.size());

        Map<String, Long> map2 = new HashMap<String, Long>();
        map2.put("A", 1L);
        map2.put("C", 2L);
        map2.put("E", 3L);
        map2.put("F", 4L);
        map2.put("G", 5L);

        Set<Map.Entry<String, Long>> set1 = map1.entrySet();
        Set<Map.Entry<String, Long>> set1Copy = new HashSet<>(map1.entrySet());
        Set<Map.Entry<String, Long>> set2 = map2.entrySet();
        System.out.println("map pairs that are in set 1 but not in set 2");
        if (set1.containsAll(set2)) {
            System.out.println("Map2 is a subset of Map1");
        }
        else {
            // expected result is key = E and value = G
            System.out.println("map pairs that are in set 1 but not in set 2");
            if(set1.retainAll(set2)){
                for(Map.Entry entry : set1){
                    System.out.println(entry.getKey() + "=" + entry.getValue());
                }
            }
        }

        if (set2.containsAll(set1)) {
            System.out.println("Map1 is a subset of Map2");
        }
        else {
            System.out.println("map pairs that are in set 2 but not in set 1");
            // expected result is key=F, value=F and key=G, value=G
            if(set2.removeAll(set1Copy)){
                for(Map.Entry entry : set2){
                    System.out.println(entry.getKey() + ":" + entry.getValue());
                }
            }
        }
    }
}
