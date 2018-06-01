package com.datatrees.datacenter.resolver.storage;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LinuxStorage implements FileStorage {
    private static Logger logger = LoggerFactory.getLogger(LinuxStorage.class);

    public Boolean commit(String source, String target) throws IOException {
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

    public OutputStream openWriter(String file) throws IOException {
        return Files.newOutputStreamSupplier(new File(file)).getOutput();
    }

    public InputStream openReader(String file) throws IOException {
        return Files.newInputStreamSupplier(new File(file)).getInput();
    }

}
