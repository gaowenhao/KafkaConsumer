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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerInterceptor {
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

        List<String> lst = new ArrayList<String>();
        lst.add("cn.gaowenhao.MyConsumerInterceptor");

        props.put("interceptor.classes", lst);
        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("mytopic-for-twoconsumer"));

        // 创建第二个消费者,配置跟第一个相同
        KafkaConsumer consumer2 = new KafkaConsumer<String, String>(props);
        consumer2.subscribe(Collections.singletonList("mytopic-for-twoconsumer"));


        new Thread(() -> {
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                for (var record : records)
                    System.out.printf("offset = %d, key = %s, value = %s, Thread-1 %n", record.offset(), record.key(), record.value());
            } while (true);
        }).start();


        new Thread(() -> {
            do {
                ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(3));
                for (var record : records)
                    System.out.printf("offset = %d, key = %s, value = %s, Thread-2 %n", record.offset(), record.key(), record.value());
            } while (true);
        }).start();
    }
}
