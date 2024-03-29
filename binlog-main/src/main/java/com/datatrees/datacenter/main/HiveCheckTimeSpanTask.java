package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.HiveCompareByFile;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.utility.StringBuilderUtil;

import java.sql.SQLException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 检查段时间内所有抓取的binlog
 */
public class HiveCheckTimeSpanTask {
    public static void main(String[] args) {
        String sql = "select file_name from t_binlog_process where process_end>'" + args[0] + "' and process_end<'" + args[1] + "' and status=0 and type='create'";
        try {
            System.out.println(sql);
            List<Map<String, Object>> fileList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), "binlog", sql);
            List<String> fileNames = new ArrayList<>();
            fileList.forEach(x -> fileNames.add(x.get("file_name").toString()));
            HiveCompareByFile hiveCompareByFile = new HiveCompareByFile();
            if (args.length == 3) {
                fileNames.parallelStream().forEach(x -> hiveCompareByFile.specialCompare(x, "create", args[2]));
            }
            if (args.length == 2) {
                fileNames.parallelStream().forEach(x -> hiveCompareByFile.binLogCompare(x, "create"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
