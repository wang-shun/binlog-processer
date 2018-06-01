package com.datatrees.datacenter.core.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileStorage {
    Boolean commit(String source, String target) throws IOException;

    OutputStream openWriter(String file) throws IOException;

    InputStream openReader(String file) throws IOException;
}
