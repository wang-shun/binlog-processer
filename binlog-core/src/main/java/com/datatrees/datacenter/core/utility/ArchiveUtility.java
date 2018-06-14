package com.datatrees.datacenter.core.utility;

import com.datatrees.datacenter.core.exception.BinlogException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveUtility {

  private static Logger logger = LoggerFactory.getLogger(ArchiveUtility.class);

  public static InputStream unArchive(String file,
    InputStream stream) {
    TarArchiveInputStream tarArchiveInputStream = null;
    try {
      tarArchiveInputStream = new TarArchiveInputStream(stream);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.
        format("error to construct tar archive input stream of %s because of %s",
          file, e.getMessage()), e
      );
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ArchiveEntry tarArchiveEntry = null;
    try {
      tarArchiveEntry = tarArchiveInputStream.getNextEntry();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.
        format("error to get archiveEntry of %s because of %s", file, e.getMessage()),
        e
      );
    }
    try {
      byte[] buffer = new byte[(int) tarArchiveEntry.getSize()];
      int count;
      while ((count = tarArchiveInputStream.read(buffer, 0, 1024)) != -1) {
        try {
          byteArrayOutputStream.write(buffer, 0, count);
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          throw new BinlogException(
            String.format("error to write stream of %s because of %s", file, e.getMessage()), e
          );
        }
      }
      ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
      return byteArrayInputStream;
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.
        format("error to read stream of %s because of %s", file, e.getMessage()),
        e
      );
    } finally {
      if (byteArrayOutputStream != null) {
        try {
          byteArrayOutputStream.close();
        } catch (IOException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }
}
