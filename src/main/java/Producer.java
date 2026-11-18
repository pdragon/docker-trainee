import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        String[] messages = {"hello", "world", "kafka", "java", "producer"};

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String msg : messages) {
                producer.send(new ProducerRecord<>("test", msg), (metadata, ex) -> {
                    if (ex == null) {
                        System.out.printf("Отправлено: '%s' -> partition=%d offset=%d%n",
                                msg, metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("Ошибка отправки: " + ex.getMessage());
                    }
                });
                Thread.sleep(500);
            }
            producer.flush();
        }
        System.out.println("Готово: все сообщения отправлены.");
    }
}
