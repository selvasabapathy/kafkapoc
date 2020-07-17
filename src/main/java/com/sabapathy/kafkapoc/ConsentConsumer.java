package com.sabapathy.kafkapoc;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.sabapathy.kafkapoc.database.DatabaseModeIdentifier;

@Service
public class ConsentConsumer
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ConsentConsumer.class);

    private static final String EARLIEST = "earliest";
    private static final String CONSUMER_GROUP_ID = "profile_changenotification";
    private static final String TOPIC_NAME = "test";

    @Value("${kafka.brconsent.servers}")
    String bootstrapAddress;

    @Autowired
    DatabaseModeIdentifier databaseModeIdentifier;

    ConcurrentMessageListenerContainer<?, ?> concurrentMessageListenerContainer;

    ConsumerFactory<String, String> consumerFactory()
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    void consume(ConsumerRecord<?, ?> consumerRecord, Acknowledgment acknowledgment)
    {
        LOGGER.info("KafkaConsumer message: " + consumerRecord);
    }

    ContainerProperties containerProperties()
    {
        ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);
        containerProperties.setMessageListener((AcknowledgingMessageListener<?, ?>) this::consume);
        return containerProperties;
    }

    ConcurrentMessageListenerContainer<?, ?> getConcurrentMessageListenerContainer()
    {
        concurrentMessageListenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory(),
                containerProperties());
        return concurrentMessageListenerContainer;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void attachConsumerAfterStartup()
    {
        if (databaseModeIdentifier.isPrimary())
        {
            getConcurrentMessageListenerContainer().start();
        }
        else
        {
            LOGGER.info("No BR Consent Consumer listener attached. The DB is on standby!");
        }
    }
}
