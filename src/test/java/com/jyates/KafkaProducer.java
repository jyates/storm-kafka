package com.jyates;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * Created by jyates on 6/20/15.
 */
public class KafkaProducer {

  private final ProducerConfig config;
  private int batchCount = 0;
  private Random rand = new Random();

  public KafkaProducer(String brokers) {
    Properties props = new Properties();
    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    config = new ProducerConfig(props);
  }

  public void send(String topic, int messageCount) {
    batchCount++;
    Producer<String, String> producer = new Producer<String, String>(config);

    for (int i = 0; i < messageCount; i++) {
      KeyedMessage<String, String> data =
          new KeyedMessage<String, String>
              (topic, batchCount + "_" + i, Integer.toString(rand.nextInt()));
      producer.send(data);
    }
    producer.close();
  }

  private Properties getProps() {

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    return props;
  }
}
