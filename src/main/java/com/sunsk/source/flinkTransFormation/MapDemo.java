package com.sunsk.source.flinkTransFormation;

import com.alibaba.fastjson.JSON;
import com.sunsk.source.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Created by sunshuangkai on 2019-06-16 17:33
 */
public class MapDemo {
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

        SingleOutputStreamOperator<Student> map = student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.id = value.id;
                s1.name = value.name;
                s1.password = value.password;
                s1.age = value.age + 100;
                return s1;
            }
        });
        map.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}

