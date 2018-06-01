package com.datatrees.datacenter.core.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Properties {

    private static Logger logger = LoggerFactory.getLogger(Properties.class);

    public static java.util.Properties load(String properties) {
        java.util.Properties props = new java.util.Properties();
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(properties));
            return props;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
