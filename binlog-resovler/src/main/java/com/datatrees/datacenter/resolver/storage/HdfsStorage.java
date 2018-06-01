package com.datatrees.datacenter.resolver.storage;

import com.datatrees.datacenter.core.storage.FileStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsStorage implements FileStorage {
    public Boolean commit(String source, String target) throws IOException {
        Boolean commit;
        Configuration conf = new Configuration();
        try {
            Path src = new Path(source);
            Path dst = new Path(target);
            FileSystem srcFileSystem = FileSystem.get(src.toUri(), conf);
            FileSystem dstFileSystem = FileSystem.get(dst.toUri(), conf);
            FileUtil.copy(srcFileSystem, src, dstFileSystem, dst, true, conf);
            commit = Boolean.TRUE;
        } catch (IllegalArgumentException e) {
            commit = Boolean.FALSE;
            e.printStackTrace();
        } catch (IOException e) {
            commit = Boolean.FALSE;
            e.printStackTrace();
        }
        return commit;
    }

    public OutputStream openWriter(String file) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        return fs.create(path, true);
    }

    public InputStream openReader(String file) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        return fs.open(path);
    }
}
