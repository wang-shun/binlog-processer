package com.datatrees.datacenter.datareader;

import java.util.Map;

public abstract class BaseDataReader {
    /**
     * 读取需要检测的文件（如果为文件的绝对路径则只读取该文件）
     *
     * @param filePath 文件路径
     */
    Map<String, Map<String, Long>> readSrcData(String filePath) {
        return null;
    }

    /**
     * 读取hive中的数据
     *
     * @param filePath
     * @return
     */
    Map<String, Long> readDestData(String filePath) {
        return null;
    }


}
