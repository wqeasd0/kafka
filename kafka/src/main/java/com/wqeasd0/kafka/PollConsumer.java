package com.wqeasd0.kafka;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created by BG292616 on 2017-08-07.
 */
public class PollConsumer extends Thread {
    private final KafkaConsumer consumer;

    private boolean running = true;

    public PollConsumer() {
        consumer = new KafkaConsumer(createConsumerConfig());
    }

    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("topic1"));
        while (running) {
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(1000);
            Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecords.iterator();
            while (iterator.hasNext()) {
                System.out.println("receive：" + new String(iterator.next().value()));
            }
            consumer.commitSync();
        }
    }

    public static void main(String[] args) {
        PollConsumer consumer = new PollConsumer();
        consumer.start();
    }
}
