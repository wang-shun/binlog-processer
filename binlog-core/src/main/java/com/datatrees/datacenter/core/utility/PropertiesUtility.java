package com.datatrees.datacenter.core.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PropertiesUtility {

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtility.class);

    public static Properties load(String properties) {
        java.util.Properties props = new java.util.Properties();
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(properties));
            return props;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
