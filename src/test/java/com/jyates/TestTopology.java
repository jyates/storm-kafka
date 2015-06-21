package com.jyates;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import org.junit.Test;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class TestTopology {
  public static final String TOPIC = "testTopic";
  public static final String SPOUT_UNQIUE_ID = "kafka-spout";
  public static final int MESSAGE_COUNT = 10;

  private static List<String> messagesReceived = new ArrayList<String>();
  public static CountDownLatch finishedCollecting = new CountDownLatch(MESSAGE_COUNT);
  private LocalCluster storm;
  private KafkaTestBroker broker;

  private BrokerHosts brokerHosts = null;
  private SpoutConfig config;

  public static void recordRecievedMessage(String msg) {
    synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
      messagesReceived.add(msg);
    }
  }

  @Test
  public void doTest() throws Exception {
    // setup the storm cluster, which also happens to start a zk cluster on 2000
    storm = new LocalCluster();

    // start a kafka cluster
    broker = new KafkaTestBroker("localhost:2000");
    setBrokerHostsAndConfig();

    // start our topology to process the records
    TopologyBuilder topology = getTopology();
    Config conf = new Config();
    conf.setDebug(true);

    storm.submitTopology("test", conf, topology.createTopology());

    // start our kafka producer and write 10 records to the kafka cluster
    KafkaProducer producer = new KafkaProducer(broker.getBrokerConnectionString());
    List<KeyedMessage<String, String>> messages = producer.send(TOPIC, MESSAGE_COUNT);

    // verify the messages are there
    verifyMessages(messages);

    // wait for all the messages to be delivered to our bolt
    finishedCollecting.await();

    storm.killTopology("test");

    // cleaup
    storm.shutdown();
    broker.shutdown();
  }

  /**
   * Verify that the messages we originally sent made it into kafka.
   * @param messages
   */
  private void verifyMessages(List<KeyedMessage<String, String>> messages) {
    SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    long lastMessageOffset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.EarliestTime());
    ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(config, simpleConsumer,
        new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), lastMessageOffset);
    int i = 0;
    for (MessageAndOffset messageAndOffset : messageAndOffsets) {
      Message kafkaMessage = messageAndOffset.message();
      ByteBuffer messageKeyBuffer = kafkaMessage.key();
      String keyString = null;
      String messageString = new String(Utils.toByteArray(kafkaMessage.payload()));
      if (messageKeyBuffer != null) {
        keyString = new String(Utils.toByteArray(messageKeyBuffer));
      }
      System.out.println("Read message => " + keyString + " : " + messageString);
      KeyedMessage<String, String> sentMessage = messages.get(i++);
      assertEquals(sentMessage.key(), keyString);
      assertEquals(sentMessage.message(), messageString);
    }
    assertEquals(i, messages.size());
  }


  private void setBrokerHostsAndConfig() {
    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
    globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
    brokerHosts = new StaticHosts(globalPartitionInformation);
    config = new SpoutConfig(brokerHosts, TOPIC, "", SPOUT_UNQIUE_ID);
    // read from the beginning of all the messages. Only needed if we have some messages already
    // stored (e.g. wrote messages before the topology was started) in which case we need to go
    // back and read them. If this isn't added, it just starts reading from the current offset
    // forward - any new messages. Since we can't be exactly quite sure when the cluster start the
    // kafka consumer, we have to add this to get any messages we send.
     config.forceFromStart = true;
  }

  private TopologyBuilder getTopology() {
    config.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(config);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", kafkaSpout, 1);
    builder.setBolt("exclaim1", new VerboseCollectorBolt(10), 1).shuffleGrouping("word");

    return builder;
  }


}
