package com.datatrees.datacenter.resolver;

import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.IPUtility;
import com.datatrees.datacenter.resolver.domain.Status;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBbiz {

  private static Logger logger = LoggerFactory.getLogger(DBbiz.class);

  public static void update(String fileName, String remarks, Status... status) {
    Map<String, Object> whereMap = new HashMap<>();
    whereMap.put("file_name", fileName);
    Map<String, Object> valueMap = new HashMap<>();
    if (status.length > 0) {
      valueMap.put("status", status[0].getValue());
    }
    valueMap.put("processor_ip", IPUtility.ipAddress());
    valueMap.put("remarks", remarks);
    try {
      DBUtil.update("t_binlog_process", valueMap, whereMap);
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("error to save status of %s", fileName));
    }
  }
}
