package com.datatrees.datacenter.rawdata;

import java.util.List;

public interface DataReader {
    void readBinLogData(String filePath);
    List<String> getFilesPath(String destPath);

}
