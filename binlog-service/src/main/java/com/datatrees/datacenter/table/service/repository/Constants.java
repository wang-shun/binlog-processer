package com.datatrees.datacenter.table.service.repository;

public interface Constants {
    public static final String CONF_DIR = System.getProperty("app.conf.dir");
    public static final String SEP = System.getProperty("file.separator");
    public static final String PROGRAM_PROP = "program.properties";
    public static final String KAFKA_PROP = "kafka.properties";

    public static final String KEY_TOPIC = "subscribe.topics";

    public static final String KAFKA_PROP_PREFIX = "binlog.service.kafka.";

}
