package com.datatrees.datacenter.transfer.utility;

import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author personalc
 */
public class BinLogFileUtil {
    private static Logger LOG = LoggerFactory.getLogger(BinLogFileUtil.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final int PAGE_SIZE = Integer.valueOf(properties.getProperty("PAGE_SIZE"));

    /**
     * 从URL中解析下载的文件名
     *
     * @param link 下载连接
     * @return 文件名
     */
    public static String getFileNameFromUrl(String link, String regex) {
        String fileName = null;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(link);
        if (matcher.find()) {
            fileName = matcher.group();
            LOG.info("fileName :"+fileName);
        } else {
            LOG.info("no fileName get from the link,please check the url or the regex pattern");
        }
        return fileName;
    }

    /**
     * 从URL中解析下载的文件编号
     *
     * @param binLogs 一批下载连接
     * @return 文件编号
     */
    public static List<Integer> getFileNumberFromUrl(List<BinLogFile> binLogs, String regex) {
        Pattern pattern = Pattern.compile(regex);
        return binLogs.stream()
                .map(binLogFile -> pattern.matcher(binLogFile.getDownloadLink()))
                .filter(Matcher::find)
                .map(matcher -> matcher.group(2))
                .map(Integer::valueOf)
                .sorted()
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * 获取binlog文件下载连接
     *
     * @param client             IAsClient
     * @param binlogFilesRequest 获取binlog下载连接请求
     * @param profile            连接阿里云参数
     * @return binlog文件下载连接
     */
    public static List<BinLogFile> getBinLogFiles(IAcsClient client, DescribeBinlogFilesRequest binlogFilesRequest, DefaultProfile profile) {
        DescribeBinlogFilesResponse binlogFilesResponse = null;
        try {
            binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
        } catch (ClientException e) {
            LOG.error("can't get binlog file from AliYun, please check the server",e);
        }
        int totalRecordCount = 0;
        if (binlogFilesResponse != null) {
            totalRecordCount = binlogFilesResponse.getTotalRecordCount();
        }
        LOG.info("totalRecordCount: " + totalRecordCount);
        List<BinLogFile> binLogFiles = new ArrayList<>(totalRecordCount);
        int pageCount = (int) Math.ceil((double) totalRecordCount / PAGE_SIZE);
        LOG.info("pageCount: " + pageCount);
        for (int i = 1; i <= pageCount; i++) {
            binlogFilesRequest.setPageNumber(i);
            try {
                binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
            } catch (ClientException e) {
                e.printStackTrace();
            }
            if (binlogFilesResponse != null) {
                int pageRecordCount = binlogFilesResponse.getPageRecordCount();
                LOG.info("PageRecordCount: " + pageRecordCount);
                List<BinLogFile> items;
                items = binlogFilesResponse.getItems();
                binLogFiles.addAll(items);
            }
        }
        BinLogFile binLogFile=new BinLogFile();
        binLogFile.getChecksum();
        return binLogFiles;
    }

    /**
     * 将binlog downloadLink 保存到filePath
     *
     * @param binLogFile binlog文件
     * @param filePath   保存文件路径
     */
    public static void saveUrlToText(BinLogFile binLogFile, String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            final boolean mkdirs = file.getParentFile().mkdirs();
        }
        // write
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            file.createNewFile();
            fw = new FileWriter(file, true);
            bw = new BufferedWriter(fw);
            String downLoadLink = binLogFile.getDownloadLink();
            bw.write(downLoadLink + "\n");
            bw.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
