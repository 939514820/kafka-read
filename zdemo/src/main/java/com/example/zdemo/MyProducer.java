package com.example.zdemo;

    import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Auther: le
 * @Date: 2019/4/23 22:05
 * @Description:
 */
public class MyProducer {
    private static KafkaProducer<String,String> producer;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    /**
     * 第一种直接发送，不管结果 有些异常捕捉不到  异步发送
     */
    private static void sendMessageForgetResult(){
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "kafka-study","name","Forget_result"
        );
        producer.send(record);
        producer.close();
    }

    /**
     * 第二种同步发送，等待执行结果 同步发送
     * @return
     * @throws Exception
     */
    private static RecordMetadata sendMessageSync() throws Exception{
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "kafka-study","name","sync"
        );
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
        return result;
    }

    /**
     * 第三种执行回调函数  异步发送
     */
    private static void sendMessageCallback(){
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(
                "kafka-study","name","callback"
        );
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                deal
            }
        });
    }
}