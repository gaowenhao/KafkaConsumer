package cn.gaowenhao;
/*
-----------------------------------------------------
    Author : 高文豪
    Github : https://github.com/gaowenhao
    Blog   : https://gaowenhao.cn
-----------------------------------------------------
*/

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {
    // 在消息被poll回之后,消费之前
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.print("onConsume 触发了.");
        return records;
    }

    // 当offset信息提交的时候
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.print("onCommit 触发了.");
    }

    // 关闭的时候
    @Override
    public void close() {
        System.out.print("onClose 触发了.");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
