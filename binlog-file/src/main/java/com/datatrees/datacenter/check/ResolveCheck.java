package com.datatrees.datacenter.check;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.transfer.process.AliBinLogFileTransfer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ResolveCheck {
    private static Logger LOG = LoggerFactory.getLogger(AliBinLogFileTransfer.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String HDFS_ROOT_PATH = properties.getProperty("AVRO_HDFS_PATH");

    public static void main(String[] args) {
        System.out.println(args.length);
        System.out.println(HDFS_ROOT_PATH);
        if (args.length != 0) {
            String filePath = args[0];
            FileSystem fs = HDFSFileUtility.getFileSystem(HDFS_ROOT_PATH);
            Path path = new Path(filePath);
            iteratorCheckFiles(fs, path);
        } else {
            LOG.error("please input file path [ eg:/data/warehouse/update]");
        }
       /* FileSystem fs = HDFSFileUtility.getFileSystem(HDFS_ROOT_PATH);
        Path path = new Path("/data/warehouse/update");
        iteratorCheckFiles(fs, path);*/
    }

    private static void iteratorCheckFiles(FileSystem fs, Path path) {
        List<String> zeroScaleIndex;
        List<Map<String, Object>> recordList;
        try {
            if (fs == null || path == null) {
                return;
            }
            FileStatus[] files = fs.listStatus(path);
            int length = files.length;
            if (length > 0) {
                List<Integer> fileNumList = new ArrayList<>();
                recordList = new ArrayList<>();
                for (int i = 0; i <= length - 1; i++) {
                    try {
                        if (files[i].isDirectory()) {
                            //递归调用
                            iteratorCheckFiles(fs, files[i].getPath());
                        } else if (files[i].isFile()) {
                            int fileNum = Integer.valueOf(files[i].getPath().toString().split("\\.")[1]);
                            fileNumList.add(fileNum);
                            if (i == length - 1) {
                                if (fileNumList.size() > 0) {
                                    Map<String, Object> recordMap;
                                    String filePath = files[i].getPath().toString();
                                    String[] filePathInfo = filePath.split("/");
                                    if (filePathInfo.length > 11) {
                                        String type = filePathInfo[5];
                                        String dbInstance = filePathInfo[6];
                                        String dataBase = filePathInfo[7];
                                        String tableName = filePathInfo[8];
                                        String year = filePathInfo[9];
                                        String month = filePathInfo[10];
                                        String day = filePathInfo[11];
                                        String partition = year + "/" + month + "/" + day;
                                        LOG.info(dbInstance + "---" + dataBase + "---" + tableName + "---" + partition);
                                        zeroScaleIndex = findUnexsistFileNum(fileNumList);
                                        if (zeroScaleIndex != null && zeroScaleIndex.size() > 0) {
                                            LOG.info("*****************");
                                            String fileUnexsist = String.join(",", zeroScaleIndex);
                                            recordMap = new HashMap<>(7);
                                            recordMap.put("type", type);
                                            recordMap.put("db_instance", dbInstance);
                                            recordMap.put("database_name", dataBase);
                                            recordMap.put("table_name", tableName);
                                            recordMap.put("file_partitions", partition);
                                            recordMap.put("un_exsist_files", fileUnexsist);
                                            recordList.add(recordMap);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                DBUtil.insertAll(DBServer.DBServerType.MYSQL.toString(), "binlog", "t_binlog_resolve_check", recordList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> findUnexsistFileNum(List<Integer> fileNumList) {
        List<String> zeroScale = null;
        if (fileNumList.size() > 0) {
            Collections.sort(fileNumList);
            int numScale = fileNumList.get(fileNumList.size() - 1) - fileNumList.get(0) + 1;
            int[] indexArr = new int[numScale];
            for (Integer fileNum : fileNumList) {
                int index = (fileNum - fileNumList.get(0)) % numScale;
                indexArr[index] = fileNum;
            }
            List<Integer> zeroIndex = new ArrayList<>();
            zeroScale = new ArrayList<>();
            for (int i = 0; i < indexArr.length; i++) {
                if (indexArr[i] == 0) {
                    zeroIndex.add(i);
                } else {
                    if (zeroIndex.size() > 0) {
                        String zeroSpan;
                        if (zeroIndex.size() > 1) {
                            zeroSpan = (fileNumList.get(0) + zeroIndex.get(0)) + "-" + (fileNumList.get(0) + zeroIndex.get(zeroIndex.size() - 1));
                        } else {
                            zeroSpan = String.valueOf(fileNumList.get(0) + zeroIndex.get(0));
                        }
                        zeroScale.add(zeroSpan);
                        zeroIndex.removeAll(zeroIndex);
                    }
                }
            }
        }
        return zeroScale;
    }
}
