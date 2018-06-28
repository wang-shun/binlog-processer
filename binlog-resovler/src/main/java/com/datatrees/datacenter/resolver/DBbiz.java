package com.datatrees.datacenter.resolver;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;

import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.IPUtility;
import com.datatrees.datacenter.resolver.partition.WriteResultValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.LinkedHashMultimap;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBbiz {

  private static Logger logger = LoggerFactory.getLogger(DBbiz.class);

  public static void update(String fileName, String remarks, Status status) {
    try {
      Builder<String, Object> builder = ImmutableMap.builder();
      if (status != Status.START) {
        builder.put("process_end", DateTime.now().toDate());
      }

      DBUtil.update("t_binlog_process",
        builder.put("processor_ip", IPUtility.ipAddress()).put("remarks", remarks)
          .put("status", status.getValue()).build(),
        ImmutableMap.<String, Object>builder().put("file_name", fileName)
          .build());
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  public static void updateLog(String file, HashMap<String, WriteResultValue> valueHashMap) {
    try {
      if (valueHashMap.size() > 0) {
        DBUtil.delete("t_binlog_process_log",
          ImmutableMap.<String, Object>builder().put("file_name", file).build());
        DBUtil
          .insertAll("t_binlog_process_log",
            newArrayList(valueHashMap.entrySet().iterator()).stream()
              .map(r -> ImmutableMap.<String, Object>builder().put("file_name", file)
                .put("db_instance", r.getKey().split("\\.")[0])
                .put("database_name", r.getKey().split("\\.")[1])
                .put("table_name", r.getKey().split("\\.")[2])
                .put("insert_cnt", r.getValue().getInsert().intValue())
                .put("update_cnt", r.getValue().getUpdate().intValue())
                .put("delete_cnt", r.getValue().getDelete().intValue())
                .put("file_partitions",
                  r.getValue().getPartitions() == null ? "" : r.getValue().getPartitions()).build()
              ).collect(toList()));
      }
    } catch (Exception e) {
      logger.error(String
        .format("error to updateLog for %s because of %s", file,
          e.getMessage()), e);
    }
  }

  public static void updatePartitions(LinkedHashMultimap<String, String> valueHashMap) {
    valueHashMap.entries().forEach(v -> {
      try {
        Map<String, Object> parameters = ImmutableMap.<String, Object>builder()
          .put("db_instance", v.getKey().split("\\.")[0])
          .put("database_name", v.getKey().split("\\.")[1])
          .put("table_name", v.getKey().split("\\.")[2])
          .put("file_partitions", v.getKey().split("\\.")[3])
          .put("avrofile", v.getValue()).build();
        DBUtil.delete(
          "t_binlog_partitions", parameters
        );
        DBUtil.insert(
          "t_binlog_partitions", parameters)
        ;
      } catch (Exception e) {
        logger.error(String
          .format("error to updatePartitions for %s because of %s", v.getKey().split("\\.")[2],
            e.getMessage()), e);
      }
    });
  }
}
