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

public class ConsumerConfig {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 指定broker
        props.put("bootstrap.servers", "localhost:9092");

        // 指定消费者分组,这个值默认是空的.
        props.put("group.id", "mygroup");

        // 消费者组之中成员保持心跳间隔
        props.put("heartbeat.interval.ms", 3000);

        // broker会和consumer之间保持心跳,若相隔一段时间broker没有收到这个consumer则会移除这个consumer, 默认:10000
        // 另外还有这两个参数可以设置：group.min.session.timeout.ms 和 group.max.session.timeout.ms
        props.put("session.timeout.ms", 10000);

        // 当使用消费者组时才有效,表达的是当前组两次poll之间最大的延时,默认:300000
        props.put("max.poll.interval.ms", 300000);

        // 是否自动提交位移信息给broker
        props.put("enable.auto.commit", "true");

        // 提交位移信息的间隔
        props.put("auto.commit.interval.ms", "1000");

        // 最小从broker取出的字节数,如果不满则等他满了
        props.put("fetch.min.bytes", 1);

        // 单次取出最大的字节,如果业务需求数据大的话可以改一下这个值
        props.put("fetch.max.bytes", 52428800);

        // 单次获取消息的数目
        props.put("max.poll.records", 500);

        // 如何选择offset有三个值可选[latest, earliest, none], 默认latest
        // latest意味着如果有上次consumer提交的offset则从那个提交的消费,如果没有则从最后
        // earliest意味着如果有上次consumer提交的offset则从那个提交的消费,如果没有则从头消费
        props.put("auto.offset.reset", "latest");


        // 序列化相关
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者对象
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

        // 订阅topic,这个方法需要的参数是Collection对象
        consumer.subscribe(Collections.singletonList("mytopic"));

        // 开启无限循环
        while (true) {
            // poll数据, 指定一个超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            // 遍历拿到的数据集合
            for (var record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }
}
