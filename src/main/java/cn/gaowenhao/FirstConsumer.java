package cn.gaowenhao;
/*
-----------------------------------------------------
    Author : 高文豪
    Github : https://github.com/gaowenhao
    Blog   : https://gaowenhao.cn
-----------------------------------------------------
*/

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FirstConsumer {
    public static void main(String[] args) {
        // 创建配置文件
        Properties props = new Properties();

        // 指定broker
        props.put("bootstrap.servers", "localhost:9092");

        // 指定消费者组
        props.put("group.id", "mygroup");

        // 指定序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者对象
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

        // 订阅topic,这个方法需要的参数是Collection对象
        consumer.subscribe(Collections.singletonList("mytopic"));

        // 开启无限循环
        do {
            // poll数据, 指定一个超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            // 遍历拿到的数据集合
            for (var record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        } while (true);
    }
}