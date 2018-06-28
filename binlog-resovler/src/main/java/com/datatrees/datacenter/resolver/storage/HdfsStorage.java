package com.datatrees.datacenter.resolver.storage;

import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.utility.ArchiveUtility;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStorage implements FileStorage {

  private static Logger logger = LoggerFactory.getLogger(HdfsStorage.class);
  private Configuration conf = null;

  private Boolean adapter;
  private FileStorage adapterFileStorage;

  private HdfsStorage() {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
//    conf.addResource(PropertiesUtility.defaultProperties().getProperty("hdfs.site"));
  }

  public HdfsStorage(FileStorage fileStorage, boolean adapter) {
    this();
    this.adapter = adapter;
    this.adapterFileStorage = fileStorage;
  }

  public Boolean commit(String source, String target) throws BinlogException {
    try {
      FileSystem fileSystem = FileSystem.get((new Path(target)).toUri(), conf);
      fileSystem.moveFromLocalFile(new Path(source), new Path(target));
      return Boolean.TRUE;
    } catch (Exception e) {
      throw new BinlogException(String.format("error to commit temp file of %s", source),
        Status.COMMITRECORDFAILED,
        e);
    }
  }

  public OutputStream openWriter(String file) throws BinlogException {
    try {
      if (adapter) {
        return adapterFileStorage.openWriter(file);
      } else {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        return fs.create(path, true);
      }
    } catch (Exception e) {
      throw new BinlogException(String.format("open writer of file %s failed.", file),
        Status.OPERAVROWRITERFAILED,
        e);
    }
  }

  public InputStream openReader(String file) throws BinlogException {
    Path path = new Path(file);
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (fs.exists(path)) {
        return ArchiveUtility.unArchive(file, fs.open(path));
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new BinlogException(String.format("open reader of file %s failed.", file),
        Status.OPENFAILED
        , e);
    }
  }

  @Override
  public Boolean exists(String file) {
    try {
      Path path = new Path(file);
      FileSystem fs = path.getFileSystem(conf);
      return fs.exists(path);
    } catch (IOException e) {
      String msg = String.format("determind open of file %s failed.",
        file);
      logger.error(msg, e);
      throw new BinlogException(msg, e);
    }
  }
}
