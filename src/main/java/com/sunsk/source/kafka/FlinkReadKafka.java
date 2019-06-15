package com.sunsk.source.kafka;




import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkReadKafka {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        Properties propsConsumer = new Properties();
        propsConsumer.setProperty("bootstrap.servers", "sunsk:9092");
        propsConsumer.setProperty("group.id", "trafficwisdom-streaming");
        propsConsumer.put("enable.auto.commit", false);
        propsConsumer.put("max.poll.records", 1000);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011("test", new SimpleStringSchema(), propsConsumer);
        consumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
        //env.execute("Flink Streaming Java API Skeleton");
    }
}
