package com.hmdp.kafka;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * 消费者
 */
@Component
public class DataKafkaConsumer {
    @KafkaListener(topics = {"data_topic"})
    public void consumer(ConsumerRecord<String, String> consumerRecord) {
        String value = consumerRecord.value();
        HashMap map = JSONUtil.toBean(value, HashMap.class);
        System.out.println(map);
        System.out.println(consumerRecord.toString());
    }

}