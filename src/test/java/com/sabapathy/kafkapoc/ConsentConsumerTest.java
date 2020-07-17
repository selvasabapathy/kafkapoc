package com.sabapathy.kafkapoc;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sabapathy.kafkapoc.database.DatabaseModeIdentifier;

@RunWith(MockitoJUnitRunner.class)
public class ConsentConsumerTest
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ConsentConsumerTest.class);

    @Mock
    DatabaseModeIdentifier databaseModeIdentifier;

    @InjectMocks
    ConsentConsumer kafkaConsumer;

    @Before
    public void setup()
    {
        initMocks(this);
    }

    @Test
    public void attachConsumerAfterStartup()
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true);
        LOGGER.debug("Bootstrap address: " + kafkaConsumer.bootstrapAddress);   // @Value fails (use ReflectionUtil?)

        kafkaConsumer.attachConsumerAfterStartup();
    }

    @Test
    public void doNotAttachConsumerAfterStartup()
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true);
        LOGGER.debug("Bootstrap address: " + kafkaConsumer.bootstrapAddress);   // @Value fails (use ReflectionUtil?)

        kafkaConsumer.attachConsumerAfterStartup();
    }
}
