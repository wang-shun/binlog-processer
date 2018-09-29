package com.datatrees.datacenter.resolver;

import static com.datatrees.datacenter.resolver.DBbiz.updateUnresolvedTempfile;
import static com.google.common.collect.Lists.newArrayList;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import java.io.File;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTempFile {

  private static Logger logger = LoggerFactory.getLogger(ListTempFile.class);

  public static void main(String[] args) {
    try {
      updateUnresolvedTempfile(newArrayList(FileUtils
        .listFiles(new File(PropertiesUtility.defaultProperties().getProperty("temp.url")),
          new String[]{"avro"}, true))
        .stream().map(r -> r.getName().replaceAll(".avro", "")).collect(Collectors.toSet()));
    } catch (Exception e) {
      logger.error("[" + "error to resolve temp file" + "]");
    }
  }
}
