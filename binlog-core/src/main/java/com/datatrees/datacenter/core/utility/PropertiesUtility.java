package com.datatrees.datacenter.core.utility;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtility {

    private static Logger logger =
            LoggerFactory.getLogger(PropertiesUtility.class);
    private static Properties __defaultProperties;

    static {
        __defaultProperties = load("binlog.properties");
    }

    public static Properties load(String properties) {
        Properties props = new Properties();
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(properties));
            return props;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static File loadResourceFile(String resourceName) {
        if (strIsRight(resourceName)) {
            URL url = ClassLoader.getSystemResource(resourceName);
            if (url != null) {
                File file = new File(url.getPath());
                logger.info("Load resource file:" + url.getPath() + " successful!");
                return file;
            } else {
                logger.error("Resource file:" + resourceName + " is not exist!");
                System.exit(1);
            }
        } else {
            logger.error("The file name is not vaild!");
        }
        return null;
    }

    private static boolean strIsRight(String str) {
        return null != str && str.length() > 0;
    }

    public static Properties defaultProperties() {
        return __defaultProperties;
    }
}
