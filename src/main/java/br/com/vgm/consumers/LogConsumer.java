package br.com.vgm.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogConsumer {

  public static void main(String[] args) {
    var consumer = new KafkaConsumer<String, String>(properties());
    consumer.subscribe(Pattern.compile("EXEMPLO.*"));

    while (true) {
      var records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> registro : records) {
        System.out.println("------------------------------------------");

        final String valor = registro.value();

        System.out.println(valor);
        System.out.println("Partition : " + registro.partition());

      }
    }
  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "LOG_CUSTOMER");
    return properties;
  }

}
