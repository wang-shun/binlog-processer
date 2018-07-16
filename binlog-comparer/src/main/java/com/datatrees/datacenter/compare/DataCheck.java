package com.datatrees.datacenter.compare;

import java.util.List;
import java.util.Map;

public interface DataCheck {
    void binLogCompare(String dest);

    List<Map<String, Object>> getCurrentPartitionInfo(String fileName);

}
