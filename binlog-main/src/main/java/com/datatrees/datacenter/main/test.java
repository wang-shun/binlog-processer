package com.datatrees.datacenter.main;

import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.datatrees.datacenter.transfer.bean.TableInfo;
import com.datatrees.datacenter.transfer.process.AliYunConfig;
import com.datatrees.datacenter.transfer.utility.BinLogFileUtil;
import com.datatrees.datacenter.transfer.utility.TimeUtil;
import jodd.typeconverter.Convert;

import java.util.Date;
import java.util.List;

public class test {
    static String start = "2018-06-19T22:50:00Z";
    static String end = "2018-06-20T04:00:00Z";
    private static final String REGEX_PATTERN = "(mysql-bin\\.)(.*)(\\.tar)";

    public static void main(String[] args) {
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setStartTime(start);
        binlogFilesRequest.setEndTime(end);
        binlogFilesRequest.setDBInstanceId("rm-bp1xvc22l77p850ja");
        List<DescribeBinlogFilesResponse.BinLogFile> binLogFiles = BinLogFileUtil.getBinLogFiles(AliYunConfig.getClient(), binlogFilesRequest, AliYunConfig.getProfile());
        if (binLogFiles.size() > 0) {
            for (DescribeBinlogFilesResponse.BinLogFile binLogFile : binLogFiles) {
                String logStart = binLogFile.getLogBeginTime();
                String logEnd = binLogFile.getLogEndTime();
                String fileName = BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN);
                System.out.println(fileName + "-" + logStart + "-" + logEnd);
                System.out.println();
                Date dt=Convert.toDate("2018-06-19 22:50:00");
                Long b=dt.getTime();
                System.out.println(b);
                System.out.println(TimeUtil.timeStamp2DateStr(b,TableInfo.UTC_FORMAT));

            }
        }
    }
}
