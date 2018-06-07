package com.datatrees.datacenter.core.storage;

import com.datatrees.datacenter.core.exception.BinlogException;

import java.io.InputStream;
import java.io.OutputStream;

public interface FileStorage {
    /**
     * 将临时文件重命名为目标文件
     * @param source temp文件
     * @param target 目标文件
     * @return
     * @throws BinlogException
     */
    Boolean commit(String source, String target) throws BinlogException;

    /**
     * 打开一个文件读写器
     * @param file
     * @return
     * @throws BinlogException
     */
    OutputStream openWriter(String file) throws BinlogException;


    /**
     * 打开一个文件读写器
     * @param file
     * @return
     * @throws BinlogException
     */
    InputStream openReader(String file) throws BinlogException;
}
