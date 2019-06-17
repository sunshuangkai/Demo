package com.sunsk.source.flinkTransFormation;

import com.alibaba.fastjson.JSON;
import com.sunsk.source.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Created by sunshuangkai on 2019-06-16 21:24
 *  student 的 age 做 KeyBy 操作分区
 * keyBy分流是按选择的key进行分流的，如果直接使用timeWindowAll只是一个分流，并行度就是1，如果keyBy使用自定义的key分流，如例，key等于1 2 3 在keyBy中可以进行取模分流，这样有可能会产生分流倾斜。
 */

public class keyByDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "sunsk:9092");
        props.put("zookeeper.connect", "sunsk:2181");
        props.put("group.id", "metric-group002");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); //value 反序列化
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "test001",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(1).map(string -> JSON.parseObject(string, Student.class));

        KeyedStream<Student, Integer> keyBy = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        });
        keyBy.print();
        env.execute("Flink keyByDemo");
    }
}