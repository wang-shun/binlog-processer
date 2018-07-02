package com.datatrees.datacenter.rawdata;

import java.util.List;
import java.util.Map;

public interface DataReader {
    /**
     * 读取指定目录下的全部文件（如果为文件的绝对路径则只读取该文件）
     *
     * @param filePath 文件路径
     */
    Map<String,List> readAllData(String filePath);

    /**
     * 获取某个文件路径下的所有文件绝对路径
     *
     * @param destPath 指定的路径
     * @return 文件路径列表
     */
    List<String> getFilesPath(String destPath);

}
