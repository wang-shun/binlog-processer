package com.datatrees.datacenter.core.storage;

import com.datatrees.datacenter.core.exception.BinlogException;
import java.io.InputStream;
import java.io.OutputStream;
public interface FileStorage {

  /**
   * 将临时文件重命名为目标文件
   *
   * @param source temp文件
   * @param target 目标文件
   */
  Boolean commit(String source, String target) throws BinlogException;

  /**
   * 打开一个文件读写器
   */
  OutputStream openWriter(String file) throws BinlogException;


  /**
   * 打开一个文件读写器
   */
  InputStream openReader(String file) throws BinlogException;

  /**
   * 判断文件是否存在
   * @param file
   * @return
   */
  Boolean exists(String file);
}
