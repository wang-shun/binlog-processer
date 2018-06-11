package com.datatrees.datacenter.schema.service.loader;

import com.datatrees.datacenter.schema.service.repository.SchemaRepository;
import com.datatrees.datacenter.schema.service.utils.StringUtils;
import io.debezium.document.DocumentReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.datatrees.datacenter.schema.service.repository.Constants.*;

public class HistoryLoader {

    private final Properties kafkaProperties = new Properties();
    private final Properties programProperties = new Properties();
    private KafkaConsumer<String, String> consumer;
    private final DocumentReader reader = DocumentReader.defaultReader();

    private volatile boolean toStop = false;

    private Set<String> subscribeTopics;

    private static Logger logger = LoggerFactory.getLogger(HistoryLoader.class);

    private SchemaRepository schemaRepository;

    private Thread thread;

    public HistoryLoader(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    public void init() throws IOException {

        //init consumer config
        configure();

        consumer = new KafkaConsumer(kafkaProperties);
        subscribeTopics = getSubscribeTopic();
        consumer.subscribe(subscribeTopics);
        logger.info("initialized history loader with config: {}, subscribed topic: {}", kafkaProperties, subscribeTopics);

        this.thread = new Thread(this::load, "history-loader");
    }

    private void configure() throws IOException {

        if (StringUtils.isEmpty(CONF_DIR)) {
            logger.info("app.conf.dir is not set use configuration from classpath");
            InputStream inputStream = this.getClass().getResourceAsStream(SEP + KAFKA_PROP);
            kafkaProperties.load(inputStream);
            inputStream.close();
        } else {
            InputStream inputStream = new FileInputStream(new File(CONF_DIR + SEP + KAFKA_PROP));
            kafkaProperties.load(inputStream);
            inputStream.close();
        }
        //init program config
        if (StringUtils.isEmpty(CONF_DIR)) {
            logger.warn("app.conf.dir is not set use configuration from classpath");
            InputStream inputStream = this.getClass().getResourceAsStream(SEP + PROGRAM_PROP);
            programProperties.load(inputStream);
            inputStream.close();
        } else {
            InputStream inputStream = new FileInputStream(new File(CONF_DIR + SEP + PROGRAM_PROP));
            programProperties.load(inputStream);
            inputStream.close();
        }
    }

    private Set<String> getSubscribeTopic() {
        subscribeTopics = consumer.listTopics().keySet();
        subscribeTopics.removeIf(this::isValidTopic);
        return subscribeTopics;
    }

    public void start() {
        this.thread.start();
    }

    public void load() {
        long loadedDDL = 0;
        while (!toStop) {
            ConsumerRecords<String, String> records = consumer.poll(60000);
            for (ConsumerRecord record : records) {
                loadedDDL++;
                loadHistory(record, schemaRepository::addSchema);
            }
            logger.info("loaded ddl count: {}", loadedDDL);
        }
    }

    private void loadHistory(ConsumerRecord<String, String> record, Consumer<HistoryRecord> recover) {
        try {
            HistoryRecord recordObj = new HistoryRecord(reader.read(record.value()));
            recover.accept(recordObj);
        } catch (Exception e) {
            logger.error("failed to load History for ddl: {}, \n {}", record.value(), e);
        }

    }

    //filter subscribe topic by regex expression
    private Boolean isValidTopic(String s) {
        try {
            String topicStr = programProperties.getProperty(KEY_TOPIC);
            String[] topicRegex = topicStr.split(",");
            for (String regex : topicRegex) {
                Pattern pattern = Pattern.compile(regex);
                if (pattern.matcher(s).matches()) {
                    return false;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }


}