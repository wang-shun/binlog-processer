package com.datatrees.datacenter.resolver;

import static com.datatrees.datacenter.resolver.DBbiz.corruptFiles;
import static com.datatrees.datacenter.resolver.DBbiz.updateCorruptFileRepair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairCorrupt {

//  private static String HDFS_ROOT = "hdfs://localhost:9000";
    private static String HDFS_ROOT = "hdfs://nameservice1:8020";
  private static Logger LOG = LoggerFactory.getLogger(RepairCorrupt.class);

  public static void main(String[] args) throws IOException {
    List<Map<String, Object>> unRepaired = corruptFiles();

    ExecutorService executorService = Executors
      .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    unRepaired.forEach(v -> {
      executorService.submit(() -> {
        FileSystem fileSystem = null;
        try {
          fileSystem = FileSystem.get((new Path(HDFS_ROOT)).toUri(), new Configuration());
        } catch (IOException e) {
          LOG.error("error to get fileSystem ", e);
        }

        Integer corrupt = Integer.valueOf(v.get("corrupt").toString());
        Boolean resolved = Boolean.FALSE;
        if (corrupt == 0) {//正常文件
          try {
            resolved = fileSystem.deleteOnExit(new Path(v.get("tmp_file_name").toString()));
          } catch (IOException e) {
            LOG.error("error to delete file " + v.get("tmp_file_name").toString(), e);
          }
        } else {
          try {
            resolved = FileUtil
              .copy(fileSystem, new Path(v.get("tmp_file_name").toString()), fileSystem,
                new Path(v.get("file_name").toString()), true, true, new Configuration());
          } catch (IOException e) {
            LOG.error("error to rename file " + v.get("file_name").toString(), e);
          }
        }
        if (resolved == Boolean.TRUE) {
          updateCorruptFileRepair(v.get("file_name").toString(),
            v.get("dir").toString(), 1);
        }
      });
    });

    try {
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      LOG.error("error to interrupt ", e);
    }
  }
}
