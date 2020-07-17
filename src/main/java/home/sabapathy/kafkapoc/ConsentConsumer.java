package home.sabapathy.kafkapoc;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import home.sabapathy.kafkapoc.database.DatabaseModeIdentifier;

@Service
@EnableScheduling
public class ConsentConsumer
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ConsentConsumer.class);

    static final String EARLIEST = "earliest";
    static final String CONSUMER_GROUP_ID = "profile_changenotification";
    static final String TOPIC_NAME = "test";

    static MessageListenerContainer listenerContainer;

    @Value("${kafka.brconsent.servers: localhost:9092}")
    String bootstrapAddress;

    @Value("${kafka.brconsent.stop: false}")
    static boolean forceStop;
    
    @Autowired
    private DatabaseModeIdentifier databaseModeIdentifier;

    public boolean isRunningOnInstanceConnectedToPrimaryDB()
    {
        return databaseModeIdentifier.isPrimary();
    }
    
    public boolean isForceStopped()
    {
        return forceStop;
    }
    
    public void forceToggle(boolean forceStop)
    {
        ConsentConsumer.forceStop = forceStop;
        toggleConsumer();
    }
    
    ConsumerFactory<String, Object> consumerFactory()
    {
        return new DefaultKafkaConsumerFactory<String, Object>(Stream.of(
            new AbstractMap.SimpleEntry<>(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress),
            new AbstractMap.SimpleEntry<>(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
            new AbstractMap.SimpleEntry<>(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
            new AbstractMap.SimpleEntry<>(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST),
            new AbstractMap.SimpleEntry<>(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),
            new AbstractMap.SimpleEntry<>(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID)
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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

    MessageListenerContainer getMessageListenerContainer()
    {
        if (listenerContainer == null)
        {
            listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory(), containerProperties());
            LOGGER.info("The GHM Db is active -- creating a BR Consent Listener!");
        }
        return listenerContainer;
    }

    boolean isListening()
    {
        return listenerContainer != null && listenerContainer.isRunning();
    }

    void startListening()
    {
        getMessageListenerContainer().start();
    }

    void stopListening()
    {
        getMessageListenerContainer().stop();
    }

    @Scheduled(fixedDelay = 500000)
    public void toggleConsumer()
    {
        if (isForceStopped() && isListening())
        {
            stopListening();
            LOGGER.info("The GHM Db is active -- force stopping the BR Consent Listener!");
            return;
        }

        LOGGER.info("Consent Consumer -- fired check and/or toggle!");
        if (isRunningOnInstanceConnectedToPrimaryDB())
        {
            if (!isListening())
            {
                startListening();
                LOGGER.info("The GHM Db is active -- (re)starting the BR Consent Listener!");
            }
        }
        else
        {
            if (isListening())
            {
                stopListening();
                LOGGER.info("The GHM Db is on standby -- stopping the BR Consent Listener!");
            }
        }
    }
}
