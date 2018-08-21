package com.datatrees.datacenter.transfer.process;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.HDFSFileUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class ResolveCheck {
    private static Logger LOG = LoggerFactory.getLogger(AliBinLogFileTransfer.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String HDFS_ROOT_PATH = properties.getProperty("AVRO_HDFS_PATH");

    public static void main(String[] args) {
        FileSystem fs = HDFSFileUtility.getFileSystem(HDFS_ROOT_PATH);
        Path path = new Path("/data/warehouse/update/debtcollection/collection/t_collection_contacts/year=2016");
        iteratorCheckFiles(fs, path);
    }

    public static void iteratorCheckFiles(FileSystem hdfs, Path path) {
        List<String> zeroScaleIndex;
        List<Map<String, Object>> recordList = null;
        try {
            if (hdfs == null || path == null) {
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            List<Integer> fileNumList = new ArrayList<>();
            //展示文件信息
            String dbInstance;
            String dataBase;
            String tableName;
            String partition;
            String year;
            String month;
            String day;
            int length = files.length;
            if (length > 0) {
                recordList = new ArrayList<>();
                for (int i = 0; i <= length - 1; i++) {
                    try {
                        if (files[i].isDirectory()) {
                            //递归调用
                            iteratorCheckFiles(hdfs, files[i].getPath());

                        } else if (files[i].isFile()) {
                            fileNumList.add(Integer.valueOf(files[i].getPath().toString().split("\\.")[1]));
                            if (i == length - 1) {
                                if (fileNumList != null && fileNumList.size() > 0) {
                                    Map<String, Object> recordMap;
                                    String filePath = files[i].getPath().toString();
                                    String[] filePathInfo = filePath.split("/");
                                    if (filePathInfo.length > 11) {
                                        dbInstance = filePathInfo[6] == null ? null : filePathInfo[6];
                                        dataBase = filePathInfo[7] == null ? null : filePathInfo[7];
                                        tableName = filePathInfo[8] == null ? null : filePathInfo[8];
                                        year = filePathInfo[9] == null ? null : filePathInfo[9];
                                        month = filePathInfo[10] == null ? null : filePathInfo[10];
                                        day = filePathInfo[11] == null ? null : filePathInfo[11];
                                        partition = year + "/" + month + "/" + day;
                                        LOG.info(dbInstance + "---" + dataBase + "---" + tableName + "---" + partition);
                                        zeroScaleIndex = findUnexsistFileNum(fileNumList);
                                        if (zeroScaleIndex.size() > 0 && zeroScaleIndex != null) {
                                            LOG.info("*****************");
                                            String fileUnexsist = String.join(",", zeroScaleIndex);
                                            recordMap = new HashMap<>(7);
                                            recordMap.put("db_instance", dbInstance);
                                            recordMap.put("database_name", dataBase);
                                            recordMap.put("table_name", tableName);
                                            recordMap.put("file_partitions", partition);
                                            recordMap.put("un_exsist_files", fileUnexsist);
                                            recordMap.put("type", "update");
                                            recordList.add(recordMap);
                                        }
                                    }
                                    fileNumList = null;
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

    static int maxGap(int A[], int n) {
        int min = A[0];
        int max = A[0];

        //找出最大值和最小值
        for (int i = 1; i < n; ++i) {
            min = (min <= A[i] ? min : A[i]);
            max = (max >= A[i] ? max : A[i]);
        }
        //记录每个桶中的最小数
        int minArr[] = new int[n + 1];
        //记录每个桶中的最大数
        int maxArr[] = new int[n + 1];
        //记录桶中是否有数
        int hasNum[] = new int[n + 1];

        for (int i = 0; i < n; ++i) {
            //求出每一个数所在的桶的编号
            int bocketID = bocketNum(A, i, min, max, n);
            minArr[bocketID] = hasNum[bocketID] == 1 ? Min(minArr[bocketID], A[i]) : A[i];
            maxArr[bocketID] = hasNum[bocketID] == 1 ? Max(maxArr[bocketID], A[i]) : A[i];
            hasNum[bocketID] = 1;
        }
        //记录最大差值
        int MaxGap = 0;
        //记录当前空桶的上一个桶的最大值
        int LastMax;

        int i = 0;
        //可能会有多个空桶
        while (i < n + 1) {
            //遍历桶，找到一个空桶
            while (i < n + 1 && hasNum[i] == 1) {
                i++;
            }
            if (i == n + 1) {
                break;
            }
            LastMax = maxArr[i - 1];
            //继续遍历桶，找到下一个非空桶
            while (i < n + 1 && hasNum[i] != 1) {
                i++;
            }
            if (i == n + 1) {
                break;
            }
            MaxGap = Max(MaxGap, minArr[i] - LastMax);
        }
        return MaxGap;
    }

    /**
     * //求出每一个数所在的桶的编号
     *
     * @param a
     * @param i
     * @param min
     * @param max
     * @param len
     * @return
     */
    static int bocketNum(int a[], int i, int min, int max, int len) {
        return (a[i] - min) * len / (max - min);
    }

    static int Min(int a, int b) {
        return a <= b ? a : b;
    }

    static int Max(int a, int b) {
        return a >= b ? a : b;
    }

    static List<String> findUnexsistFileNum(List<Integer> fileNumList) {
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
                    if (zeroIndex.size() > 0 && zeroIndex != null) {
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
