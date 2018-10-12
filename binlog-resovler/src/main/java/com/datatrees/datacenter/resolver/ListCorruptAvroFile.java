package com.datatrees.datacenter.resolver;

import static com.datatrees.datacenter.resolver.DBbiz.updateCorruptFiles;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ListCorruptAvroFile implements Runnable {

  private String partition;

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      return;
    }

    ListCorruptAvroFile avroFile = new ListCorruptAvroFile();
//    avroFile.partition = args[0];
    avroFile.partition = "/data/warehouse/create/basisdataoperator/operator/t_tel_call_sheet/year=2017/month=9/day=6/1538590505-mysql-bin.001211.avro";
    avroFile.run();
  }

  private static void validateCorruptFiles(DataFileStream reader, Path path) throws Exception {
    try {
      while (reader.hasNext()) {
        reader.next();
      }
    } catch (Exception e) {
      System.out
        .println("corrupt avro file : [" + path == null ? "[null ]" : path.getName() + "]");
      throw e;
    }
  }

  @Override
  public void run() {
    BufferedInputStream inStream = null;
    Path path =  new org.apache.hadoop.fs.Path(partition);;
    List<String> curruptFiles = new ArrayList<>();
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://cloudera2:8020/");
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(partition), conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    RemoteIterator<LocatedFileStatus> remoteIterator = null;
    try {
      remoteIterator = fs.listFiles(path, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      while (remoteIterator.hasNext()) {
        path = remoteIterator.next().getPath();
        inStream = new BufferedInputStream(fs.open(path));
        DataFileStream reader =
          null;
        try {
          reader = new DataFileStream(inStream, new GenericDatumReader());
        } catch (IOException e) {
          e.printStackTrace();
        }
        try {
          validateCorruptFiles(reader, path);
        } catch (Exception e) {
          if (null != path) {
            curruptFiles.add(path.getName());
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (curruptFiles.size() > 0) {
        updateCorruptFiles(partition, curruptFiles);
      }
    }
  }
}
