package com.sabapathy.kafkapoc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@Configuration
@EnableKafka
public class KafkaConsumer
{
    private final static String TOPIC_NAME = "test";

    @KafkaListener(topics = TOPIC_NAME, containerFactory = "defaultKafkaListenerContainerFactory")
    public void consume(ConsumerRecord<?, ?> consumerRecord, Acknowledgment acknowledgment) throws Exception
    {
        System.out.println("KafkaConsumer message: " + consumerRecord);
        acknowledgment.acknowledge();
    }
}
