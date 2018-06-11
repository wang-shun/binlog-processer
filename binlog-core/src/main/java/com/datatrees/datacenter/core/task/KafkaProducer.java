package com.datatrees.datacenter.core.task;

import java.io.Serializable;
import java.util.Properties;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zxding1986 on 17/11/17.
 */
public class KafkaProducer implements Cloneable, Serializable {

    private static org.apache.kafka.clients.producer.KafkaProducer kafkaProducer;
    private static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private static String queueTopic;

    static {
        Properties props = PropertiesUtility.defaultProperties();
        queueTopic = props.getProperty("task.queue");
        props.put("bootstrap.servers", props.getProperty("bootstrap.servers"));//该地址是集群的子集，用来探测集群。
        props.put("acks", "all");// 记录完整提交，最慢的但是最大可能的持久化
        props.put("retries", 3);// 请求失败重试的次数
        props.put("batch.size", 16384);// batch的大小
        props.put("linger.ms",
                1);// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
        props.put("buffer.memory", 33554432);// 提供给生产者缓冲内存总量
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，
        // ByteArraySerializer或者StringSerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(
                props);
    }

    public <T> void send(T t) {
        try {
            if (kafkaProducer != null) {
                kafkaProducer.send(new ProducerRecord(queueTopic, t));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new IllegalArgumentException(String
                    .format("send message to kafka message midware failed because of the following message ",
                            e.getMessage()));
        } finally {
            if (kafkaProducer != null) {
            }
        }
    }
}
