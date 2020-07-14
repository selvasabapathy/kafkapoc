package com.sabapathy.kafkapoc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer
{
    private static final String TOPIC_NAME = "test";

    @Autowired
    ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;

    @Value("${kafka.brconsent.listener.enable}")
    private boolean attachListener;

    @EventListener(ApplicationReadyEvent.class)
    public void afterStartup()
    {
        if (!attachListener)
        {
            // Find out from DatabaseModeIdentifier's isPrimary()... assume true for now!
            attachListener = true;
            System.out.println("kafka.brconsent.listener.enable (after Db check): " + attachListener);
        }

        ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);
        containerProperties.setMessageListener(
                (AcknowledgingMessageListener<?, ?>) (consumerRecord, acknowledgment) -> System.out
                        .println("KafkaConsumer message: " + consumerRecord)
        );

        ConcurrentMessageListenerContainer listenerContainer = new ConcurrentMessageListenerContainer<>(
                containerFactory.getConsumerFactory(), containerProperties);
        listenerContainer.start();
    }
}
