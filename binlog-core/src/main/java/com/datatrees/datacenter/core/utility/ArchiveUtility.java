package com.datatrees.datacenter.core.utility;

import com.datatrees.datacenter.core.exception.BinlogException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveUtility {

  private static Logger logger = LoggerFactory.getLogger(ArchiveUtility.class);

  public static InputStream unArchive(String file, InputStream stream) {
    try {
      TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(stream);
      ArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextEntry();
      byte[] buffer = new byte[(int) tarArchiveEntry.getSize()];
      IOUtils.readFully(tarArchiveInputStream, buffer);
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
      return byteArrayInputStream;
    } catch (Exception e) {
      String msg =
        String.format("error to archive tar archive input stream of %s because of %s",
          file, e.getMessage());
      logger.error(e.getMessage(), e);
      throw new BinlogException(msg, e);
    }
  }
}
