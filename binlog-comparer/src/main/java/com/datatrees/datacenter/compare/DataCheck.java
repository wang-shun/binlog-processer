package com.datatrees.datacenter.compare;

import java.util.List;
import java.util.Map;

public interface DataCheck {
    void binLogCompare(String src, String dest);

    List<Map<String, Object>> getCurrentPartitinInfo(String fileName);
}
