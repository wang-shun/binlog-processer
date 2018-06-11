import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/5/25 17:40
 * @Modified By:
 */
public class ConsumerTest {
    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka2:9092");
        map.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-comsumer");
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "test-comsumer-group1");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(map);
        Set<String> topics = new HashSet<>();
        topics.add("connect-offsets");
        consumer.subscribe(topics);

        ConsumerRecords<String,String> record = consumer.poll(10000);
        for (ConsumerRecord record1 : record){
            System.out.println(record1.key().toString());
        }
    }
}
