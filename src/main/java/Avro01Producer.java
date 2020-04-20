import com.cbx.model.avro.Alert;
import com.cbx.model.avro.alert_status;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Calendar;
import java.util.Properties;

public class Avro01Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", LongSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");


        Producer<Long, Alert> producer = new KafkaProducer<Long, Alert>(props);
        Alert alert = new Alert();
        alert.setSensorId(12345L);
        alert.setTime(Calendar.getInstance().getTimeInMillis());
        alert.setStatus(alert_status.Critical);
        System.out.println(alert.toString());

        ProducerRecord<Long, Alert> producerRecord = new ProducerRecord<Long, Alert>("avrotest", alert.getSensorId(),
                alert);

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e ==null){
                    System.out.println("Success");
                }else {
                    e.printStackTrace();
                }


            }
        });

        producer.close();
    }
}
