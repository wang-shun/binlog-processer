package com.datatrees.datacenter.transfer.utility;

import com.datatrees.datacenter.transfer.bean.HttpAccessStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author personalc
 */
public class FileUtil {
    private static Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    /**
     * 获取网络文件的大小
     *
     * @return 文件大小
     */
    public long getFileSize(String path) {
        int len = -1;
        try {
            URL url = new URL(path);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("User-Agent", "custom");

            int respCode = connection.getResponseCode();
            if (respCode >= HttpAccessStatus.HTTP_CONNECTION_RESPONSE_CODE.getValue()) {
                LOG.info("Error Code : " + respCode);
                // 代表文件不可访问
                return HttpAccessStatus.FILE_NOT_ACCESSIBLE.getValue();
            }

            String header;
            for (int i = 1; ; i++) {
                header = connection.getHeaderFieldKey(i);
                if (header != null) {
                    if ("Content-Length".equals(header)) {
                        len = Integer.parseInt(connection.getHeaderField(header));
                        break;
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }
        LOG.info("the real size of this file :" + path + " is :  " + len);
        return len;
    }
}