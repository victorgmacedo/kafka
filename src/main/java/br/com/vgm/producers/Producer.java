package br.com.vgm.producers;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import br.com.vgm.dto.Customer;

public class Producer {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    var producer = new KafkaProducer<String, Customer>(properties());
    for (int i = 0; i < 5; i++) {
      var customer = new Customer("Victor" + new Random().nextInt(), 16 + i);
      var record = new ProducerRecord<String, Customer>("EXEMPLO_CUSTOMER", customer.getNome(), customer);
      Callback callback = (data, ex) -> {
        if (ex != null) {
          ex.printStackTrace();
          return;
        }
        System.out.println("Mensagem enviada com sucesso para: " + data.topic() + " | partition " + data.partition() + "| offset " + data.offset() + "| tempo " + data.timestamp());
      };
      producer.send(record, callback).get();
    }

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
    return properties;
  }
}
