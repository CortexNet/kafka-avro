import com.cbx.model.avro.Alert;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class Avro01Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", "helloconsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        @SuppressWarnings("resource")
        KafkaConsumer<Long, Alert> consumer = new KafkaConsumer<Long, Alert>(props);
        consumer.subscribe(Arrays.asList("avrotest"));

        while (true) {
            ConsumerRecords<Long, Alert> records = consumer.poll(100);
            for (ConsumerRecord<Long, Alert> record : records)
                System.out.printf("offset = %d, key = %d, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
