package com.datatrees.datacenter.core.storage;

import com.datatrees.datacenter.core.exception.BinlogException;

import java.io.InputStream;
import java.io.OutputStream;

public interface FileStorage {
    Boolean commit(String source, String target) throws BinlogException;

    OutputStream openWriter(String file) throws BinlogException;

    InputStream openReader(String file) throws BinlogException;
}
