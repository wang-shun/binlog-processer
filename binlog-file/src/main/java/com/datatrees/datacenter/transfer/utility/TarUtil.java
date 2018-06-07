package com.datatrees.datacenter.transfer.utility;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author personalc
 */
public class TarUtil {
    /**
     * 压缩文件
     *
     * @param name
     * @param files
     * @throws IOException
     */
    public static void compress(String name, File... files) throws IOException {
        try (TarArchiveOutputStream out = getTarArchiveOutputStream(name)) {
            for (File file : files) {
                addToArchiveCompression(out, file, ".");
            }
        }
    }

    /**
     * 解压文件
     *
     * @param in
     * @param out
     * @throws IOException
     */
    public static void decompress(String in, File out) throws IOException {
        try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(in))) {
            TarArchiveEntry entry;
            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(out, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                IOUtils.copy(fin, new FileOutputStream(curfile));
            }
        }
    }

    /**
     * @param name
     * @return
     * @throws IOException
     */
    private static TarArchiveOutputStream getTarArchiveOutputStream(String name) throws IOException {
        TarArchiveOutputStream taos = new TarArchiveOutputStream(new FileOutputStream(name));
        // TAR has an 8 gig file limit by default, this gets around that
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
        // TAR originally didn't support long file names, so enable the support for it
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        taos.setAddPaxHeadersForNonAsciiNames(true);
        return taos;
    }

    /**
     * 添加文件至压缩文件
     *
     * @param out
     * @param file
     * @param dir
     * @throws IOException
     */
    private static void addToArchiveCompression(TarArchiveOutputStream out, File file, String dir) throws IOException {
        String entry = dir + File.separator + file.getName();
        if (file.isFile()) {
            out.putArchiveEntry(new TarArchiveEntry(file, entry));
            try (FileInputStream in = new FileInputStream(file)) {
                IOUtils.copy(in, out);
            }
            out.closeArchiveEntry();
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addToArchiveCompression(out, child, entry);
                }
            }
        } else {
            System.out.println(file.getName() + " is not supported");
        }
    }


}
