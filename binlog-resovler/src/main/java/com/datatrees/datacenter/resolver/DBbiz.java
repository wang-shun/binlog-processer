package com.datatrees.datacenter.resolver;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;

import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.IPUtility;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.partition.WriteResultValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.LinkedHashMultimap;
import io.prometheus.client.Counter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBbiz {

  private static Logger logger = LoggerFactory.getLogger(DBbiz.class);
  private static Properties properties = PropertiesUtility.defaultProperties();
  private static String dataBase = properties.getProperty("jdbc.database");
  private static String DBType = DBServer.DBServerType.MYSQL.toString();

  private static Counter counter = Counter.build("binlog_resolve_final_status", "binlog文件最终处理状态")
    .labelNames("type").register();

  public static void update(String fileName, String remarks, Status status) {
    try {
      Builder<String, Object> builder = ImmutableMap.builder();
      if (status != Status.START) {
        builder.put("process_end", DateTime.now().toDate());
      }

      DBUtil.update(DBType, dataBase, "t_binlog_process",
        builder.put("processor_ip", IPUtility.ipAddress())
          .put("remarks", remarks == null ? "null" : remarks)
          .put("status", status.getValue()).build(),
        ImmutableMap.<String, Object>builder().put("file_name", fileName)
          .build());

      try {
        switch (status) {
          case SUCCESS:
            counter.labels("success").inc();
            break;
          case OPENFAILED:
          case OPERAVROWRITERFAILED:
          case SERIALIZEEVENTFAILED:
          case SCHEMAFAILED:
          case RESOVLERECORDFAILED:
          case WRITERECORDFAILED:
          case COMMITRECORDFAILED:
          case OTHER:
            counter.labels("fail").inc();
            break;
          default:
            break;
        }
      } catch (Exception e) {
        logger.error(
          String.format("error to give metrics of %s because of %s", fileName, e.getMessage()), e);
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  public static void updateLog(String file, HashMap<String, WriteResultValue> valueHashMap) {

    try {
      DBUtil.delete(DBType, dataBase, "t_binlog_process_log", ImmutableMap.<String, Object>builder()
        .put("file_name", file).build());
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }
    try {
      DBUtil
        .insertAll(DBType, dataBase, "t_binlog_process_log",
          newArrayList(valueHashMap.entrySet().iterator()).stream()
            .map(r -> ImmutableMap.<String, Object>builder().put("file_name", file)
              .put("type", r.getKey().split("\\.")[0])
              .put("db_instance", r.getKey().split("\\.")[1])
              .put("database_name", r.getKey().split("\\.")[2])
              .put("table_name", r.getKey().split("\\.")[3])
              .put("file_partitions", r.getKey().split("\\.")[4])
              .put("insert_cnt", r.getValue().getInsert().intValue())
              .put("update_cnt", r.getValue().getUpdate().intValue())
              .put("delete_cnt", r.getValue().getDelete().intValue()).build()
            ).collect(toList()));
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
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
        DBUtil.delete(DBType, dataBase, "t_binlog_partitions", parameters);
        DBUtil.insert(DBType, dataBase, "t_binlog_partitions", parameters);
      } catch (Exception e) {
        logger.error(String
          .format("error to updatePartitions for %s because of %s", v.getKey().split("\\.")[2],
            e.getMessage()), e);
      }
    });
  }

  public static void report(String topic, int lqs, long tc, long tcc, int ps, int cps, int pac,
    int mps, int sap,
    int sql) {
    try {
      DBUtil.insert(DBType, dataBase, "t_binlog_process_report",
        ImmutableMap.<String, Object>builder()
          .put("topic", topic == null ? "null" : topic)
          .put("local_queue_size", lqs)
          .put("thread_pool_task_count", tc)
          .put("thread_pool_task_completed_count", tcc)
          .put("thread_pool_size", ps)
          .put("thread_pool_core_pool_size", cps)
          .put("thread_pool_active_count", pac)
          .put("thread_pool_max_pool_size", mps)
          .put("sempahore_available_permits", sap)
          .put("sempahore_queue_length", sql)
          .put("create_date", DateTime.now().toDate())
          .put("prossesor_ip", IPUtility.ipAddress())
          .build());
    } catch (Exception e) {
      logger.error(String
        .format("error to report status"), e);
    }
  }
}
