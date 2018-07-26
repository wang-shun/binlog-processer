package com.datatrees.datacenter.resolver;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.storage.HdfsStorage;
import java.io.File;
import java.util.Collection;
import org.apache.commons.io.FileUtils;

public class FixTempFileTask {

  public static void main(String[] args) {

    FileStorage fileStorage = new HdfsStorage();
    java.util.Properties value = PropertiesUtility.defaultProperties();
    String tempUrl = value.getProperty("temp.url");
    String warehouseUrl = value.getProperty("hdfs.warehouse.url");

    Collection<File> tempAvroFiles =
      FileUtils.listFiles(new File(tempUrl), new String[]{"avro"}, true);
    tempAvroFiles.forEach(file -> {

      String tempPath = file.getPath();
      String targetPath =
        tempPath.replace(tempUrl, warehouseUrl)
          .replace(tempPath.split("/")[6] + "/", "");

      String[] splitTargetPath = targetPath.split("/");
      if (args.length == 1) {
        if (args[0].equalsIgnoreCase(splitTargetPath[7])) {
          System.out.println(targetPath);
          fileStorage.commit(tempPath, targetPath);
        }
      } else if (args.length == 2) {
        if (args[0].equalsIgnoreCase(splitTargetPath[7]) &&
          args[1].equalsIgnoreCase(splitTargetPath[8])
          ) {
          System.out.println(targetPath);
          fileStorage.commit(tempPath, targetPath);
        }
      } else if (args.length == 3) {
        if (args[0].equalsIgnoreCase(splitTargetPath[7]) && args[1]
          .equalsIgnoreCase(splitTargetPath[8]) && String
          .format("%s/%s/%s", splitTargetPath[9], splitTargetPath[10], splitTargetPath[11])
          .equalsIgnoreCase(args[2])
          ) {
          System.out.println(targetPath);
          fileStorage.commit(tempPath, targetPath);
        }
      } else {
        System.out.println(targetPath);
        fileStorage.commit(tempPath, targetPath);
      }
    });
  }
}
