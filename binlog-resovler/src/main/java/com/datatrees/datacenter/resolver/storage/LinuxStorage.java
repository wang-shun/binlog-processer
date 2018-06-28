package com.datatrees.datacenter.resolver.storage;

import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinuxStorage implements FileStorage {

  private static Logger logger = LoggerFactory.getLogger(LinuxStorage.class);

  public Boolean commit(String source, String target) throws BinlogException {
    Boolean success = Boolean.FALSE;
    try {
      File file = new File(source);
      File dir = new File(target);
      dir.getParentFile().mkdirs();
      success = file.renameTo(dir);
      file.delete();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      success = Boolean.FALSE;
    }

    return success;
  }

  public OutputStream openWriter(String filePath) throws BinlogException {
    try {
      File file = new File(filePath);
      file.getParentFile().mkdirs();
      return Files.newOutputStreamSupplier(file).getOutput();
    } catch (IOException e) {
      throw new BinlogException(String.format("open linux reader of file %s failed.", filePath),
        Status.OPENFAILED, e);
    }
  }

  public InputStream openReader(String file) throws BinlogException {
    try {
      return Files.newInputStreamSupplier(new File(file)).getInput();
    } catch (IOException e) {
      throw new BinlogException(String.format("open linux reader of file %s failed.", file),
        Status.OPENFAILED, e);
    }
  }

  @Override
  public Boolean exists(String file) {
    return null;
  }
}
