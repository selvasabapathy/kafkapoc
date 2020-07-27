package home.sabapathy.kafkapoc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import home.sabapathy.kafkapoc.database.DatabaseModeIdentifier;

@RunWith(MockitoJUnitRunner.class)
public class ConsentConsumerTest
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ConsentConsumerTest.class);

    @Mock
    DatabaseModeIdentifier databaseModeIdentifier;

    @Mock
    ConcurrentMessageListenerContainer listenerContainer;

    @InjectMocks
    @Spy
    ConsentConsumer consentConsumer;

    @Before
    public void setUp()
    {
        ReflectionTestUtils.setField(consentConsumer, "bootstrapAddress", "localhost:9022");
        consentConsumer.forceStop = false;
        consentConsumer.listenerContainer = null;
    }

    @After
    public void tearDown()
    {
        verifyNoMoreInteractions(consentConsumer);
    }

    @Test
    public void onActiveDbCreateConsumerAndStart()  // cold start
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true);
        
        assertNull("Shouldn't there be a consumer on cold start", consentConsumer.listenerContainer);

        // Fire a scheduler event - Create and attach the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer should be created", consentConsumer.listenerContainer);
        assertTrue("Consumer should be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer).toggleConsumer();
        
        verify(consentConsumer).isForceStopped();
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer).isListening();
        verify(consentConsumer).startListening();
        verify(consentConsumer).getMessageListenerContainer();
        verify(consentConsumer).consumerFactory();
        verify(consentConsumer).containerProperties();
    }

    @Test
    public void onActiveDbStartIdleConsumer()   // switch to active DB - start the listener
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true).thenReturn(false).thenReturn(true);

        // ACTIVE - Fire a scheduler event - Create and attach the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer should be created", consentConsumer.listenerContainer);

        verify(consentConsumer).toggleConsumer();

        verify(consentConsumer).isForceStopped();
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer).isListening();
        verify(consentConsumer).startListening();
        verify(consentConsumer).getMessageListenerContainer();
        verify(consentConsumer).consumerFactory();
        verify(consentConsumer).containerProperties();
        
        // STAND BY - Fire a scheduler event - Stop the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer shouldn't be destroyed", consentConsumer.listenerContainer);
        assertFalse("Consumer shouldn't be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer, times(2)).toggleConsumer();

        verify(consentConsumer, times(2)).isForceStopped();
        verify(consentConsumer, times(2)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(2)).isPrimary();
        verify(consentConsumer, times(2)).isListening();
        verify(consentConsumer).stopListening();

        // ACTIVE again - Fire a scheduler event - attach the listener
        consentConsumer.toggleConsumer();
        assertTrue("Consumer should be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer, times(3)).toggleConsumer();

        verify(consentConsumer, times(3)).isForceStopped();
        verify(consentConsumer, times(3)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(3)).isPrimary();
        verify(consentConsumer, times(3)).isListening();
        verify(consentConsumer, times(2)).startListening();
        verify(consentConsumer, times(3)).getMessageListenerContainer();
    }

    @Test
    public void onActiveDbDoNothingToListeningConsumer()  // active DB and running listener, do nothing
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true);

        // Fire a scheduler event - Create and attach the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer should be created", consentConsumer.listenerContainer);
        assertTrue("Consumer should be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer).toggleConsumer();

        verify(consentConsumer).isForceStopped();
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer).isListening();
        verify(consentConsumer).startListening();
        verify(consentConsumer).getMessageListenerContainer();
        verify(consentConsumer).consumerFactory();
        verify(consentConsumer).containerProperties();

        // Fire a scheduler event - do nothing
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer shouldn't be destroyed", consentConsumer.listenerContainer);
        assertTrue("Consumer should still be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer, times(2)).toggleConsumer();

        verify(consentConsumer, times(2)).isForceStopped();
        verify(consentConsumer, times(2)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(2)).isPrimary();
        verify(consentConsumer, times(2)).isListening();
    }

    @Test
    public void onStandbyDbDonotCreateConsumer()    // cold start
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(false);

        assertNull("Shouldn't there be a consumer on cold start", consentConsumer.listenerContainer);

        // Fire a scheduler event - Shouldn't create or attach a listener
        consentConsumer.toggleConsumer();

        assertNull("Consumer shouldn't be created", consentConsumer.listenerContainer);
        assertFalse("Consumer shouldn't be listening", consentConsumer.isListening());
        
        verify(consentConsumer).toggleConsumer();
        verify(consentConsumer).isForceStopped();   
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer, times(2)).isListening();
    }

    @Test
    public void onStandbyDbStopListeningConsumer()  // switch to standby DB - stop the listener
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true).thenReturn(false);

        // ACTIVE - Fire a scheduler event - Create and attach the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer should be created", consentConsumer.listenerContainer);

        verify(consentConsumer).toggleConsumer();

        verify(consentConsumer).isForceStopped();
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer).isListening();
        verify(consentConsumer).startListening();
        verify(consentConsumer).getMessageListenerContainer();
        verify(consentConsumer).consumerFactory();
        verify(consentConsumer).containerProperties();

        // STAND BY - Fire a scheduler event - Stop the listener
        consentConsumer.toggleConsumer();
        assertFalse("Consumer shouldn't be listening", consentConsumer.listenerContainer.isRunning());
        assertNotNull("Consumer shouldn't be destroyed", consentConsumer.listenerContainer);

        verify(consentConsumer, times(2)).toggleConsumer();

        verify(consentConsumer, times(2)).isForceStopped();
        verify(consentConsumer, times(2)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(2)).isPrimary();
        verify(consentConsumer, times(2)).isListening();
        verify(consentConsumer).stopListening();
        verify(consentConsumer, times(2)).getMessageListenerContainer();
    }

    @Test
    public void onStandbyDbDoNothingToIdleConsumer()      // standby listener, do nothing
    {
        when(databaseModeIdentifier.isPrimary()).thenReturn(true).thenReturn(false);

        // ACTIVE - Fire a scheduler event - Create and attach the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer should be created", consentConsumer.listenerContainer);

        verify(consentConsumer).toggleConsumer();

        verify(consentConsumer).isForceStopped();
        verify(consentConsumer).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier).isPrimary();
        verify(consentConsumer).isListening();
        verify(consentConsumer).startListening();
        verify(consentConsumer).getMessageListenerContainer();
        verify(consentConsumer).consumerFactory();
        verify(consentConsumer).containerProperties();

        // STAND BY - Fire a scheduler event - Stop the listener
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer shouldn't be destroyed", consentConsumer.listenerContainer);
        assertFalse("Consumer shouldn't be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer, times(2)).toggleConsumer();

        verify(consentConsumer, times(2)).isForceStopped();
        verify(consentConsumer, times(2)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(2)).isPrimary();
        verify(consentConsumer, times(2)).isListening();
        verify(consentConsumer).stopListening();
        verify(consentConsumer, times(2)).getMessageListenerContainer();

        // Fire a scheduler event - do nothing
        consentConsumer.toggleConsumer();
        assertNotNull("Consumer shouldn't be destroyed", consentConsumer.listenerContainer);
        assertFalse("Consumer shouldn't still be listening", consentConsumer.listenerContainer.isRunning());

        verify(consentConsumer, times(3)).toggleConsumer();

        verify(consentConsumer, times(3)).isForceStopped();
        verify(consentConsumer, times(3)).isRunningOnInstanceConnectedToPrimaryDB();
        verify(databaseModeIdentifier, times(3)).isPrimary();
        verify(consentConsumer, times(3)).isListening();
    }
}
