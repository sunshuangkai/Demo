package com.sunsk.source.flinkTransFormation;


import com.alibaba.fastjson.JSON;
import com.sunsk.source.Student;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Created by sunshuangkai on 2019-06-16 21:51
 * Fold 通过将最后一个文件夹流与当前记录组合来推出 KeyedStream。 它会发回数据流。
 */
public class FoldDemo {
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

//        KeyedStream.fold("1", new FoldFunction<Integer, String>() {
//            @Override
//            public String fold(String accumulator, Integer value) throws Exception {
//                return accumulator + "=" + value;
//            }
//        });
        env.execute("Flink Streaming Java API Skeleton");
    }
}
