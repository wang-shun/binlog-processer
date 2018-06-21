package com.datatrees.datacenter.resolver.storage;

import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.utility.ArchiveUtility;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStorage implements FileStorage {

  private static Logger logger = LoggerFactory.getLogger(HdfsStorage.class);
  private Configuration conf = null;

  public HdfsStorage() {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
  }

  public Boolean commit(String source, String target) throws BinlogException {
    Path src = new Path(source);
    Path dst = new Path(target);
    try {
      FileSystem srcFileSystem = FileSystem.get(src.toUri(), conf);
      FileSystem dstFileSystem = FileSystem.get(dst.toUri(), conf);
      FileUtil.copy(srcFileSystem, src, dstFileSystem, dst, true, false, conf);
      return Boolean.TRUE;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("error to commit temp file of %s", source));
//            return Boolean.FALSE;
    }
  }

  public OutputStream openWriter(String file) throws BinlogException {
    Path path = new Path(file);
    try {
      FileSystem fs = path.getFileSystem(conf);
      return fs.create(path, true);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("open writer of file %s failed.", file));
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
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("open reader of file %s failed.",
        file)
      );
    }
  }

  @Override
  public Boolean exists(String file) {
    try {
      Path path = new Path(file);
      FileSystem fs = path.getFileSystem(conf);
      return fs.exists(path);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("determind open of file %s failed.",
        file)
      );
    }
  }
}
