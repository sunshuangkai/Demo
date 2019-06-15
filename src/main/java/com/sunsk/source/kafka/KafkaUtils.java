package com.sunsk.source.kafka;

import com.alibaba.fastjson.JSON;
import com.sunsk.source.wordcount.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 往kafka中写数据
 * 可以使用这个main函数进行测试一下
 */
public class KafkaUtils {
    public static final String broker_list = "sunsk:9092";
    public static final String topic = "metric";  // kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap();
        Map<String, Object> fields = new HashMap();

        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "10.211.55.4");
        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);
        ProducerRecord record = new ProducerRecord<String, String>("test", null, null, JSON.toJSONString(metric));
        try {
            producer.send(record); //2
        } catch (Exception e) {
            e.printStackTrace(); //3
        }
        System.out.println("发送数据: " + JSON.toJSONString(metric));
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
