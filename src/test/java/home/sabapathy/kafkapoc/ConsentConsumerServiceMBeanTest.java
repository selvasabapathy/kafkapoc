package home.sabapathy.kafkapoc;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsentConsumerServiceMBeanTest
{
    @Mock
    ConsentConsumer consentConsumer;

    @InjectMocks
    ConsentConsumerServiceMBean consentConsumerServiceMBean;

    @Before
    public void setup()
    {
        doNothing().when(consentConsumer).startListening();
        doNothing().when(consentConsumer).stopListening();
    }

    @After
    public void tearDown()
    {
        verifyNoMoreInteractions(consentConsumer);
    }

    @Test
    public void onActiveDbIsConsumerListening()
    {
        when(consentConsumer.isListening()).thenReturn(true);

        assertThat("Should be listening", consentConsumerServiceMBean.isConsumerListening(), is(true));
        verify(consentConsumer).isListening();
    }

    @Test
    public void onActiveDbStartConsumer()
    {
        when(consentConsumer.isRunningOnInstanceConnectedToPrimaryDB()).thenReturn(true);
        when(consentConsumer.isListening()).thenReturn(true);
        doNothing().when(consentConsumer).forceToggle(false);
        
        assertThat("Should have been started", consentConsumerServiceMBean.startConsumer(), is(true));
        verify(consentConsumer).forceToggle(false);
        verify(consentConsumer).isListening();
    }

    @Test
    public void onActiveDbStopConsumer()
    {
        when(consentConsumer.isRunningOnInstanceConnectedToPrimaryDB()).thenReturn(true);
        when(consentConsumer.isListening()).thenReturn(false);
        doNothing().when(consentConsumer).forceToggle(true);
        
        assertThat("Should have been stopped", consentConsumerServiceMBean.stopConsumer(), is(false));
        verify(consentConsumer).forceToggle(true);
        verify(consentConsumer).isListening();
    }

    @Test
    public void onStandbyDbIsConsumerListening()
    {
        when(consentConsumer.isListening()).thenReturn(false);

        assertThat("Shouldn't be listening", consentConsumerServiceMBean.isConsumerListening(), is(false));
        verify(consentConsumer).isListening();
    }

    @Test
    public void onStandbyDbStartConsumer()
    {
        when(consentConsumer.isRunningOnInstanceConnectedToPrimaryDB()).thenReturn(false);
        when(consentConsumer.isListening()).thenReturn(false);
        doNothing().when(consentConsumer).forceToggle(false);

        assertThat("Shouldn't have been started", consentConsumerServiceMBean.startConsumer(), is(false));
        verify(consentConsumer).forceToggle(false);
        verify(consentConsumer).isListening();
    }

    @Test
    public void onStandbyDbStopConsumer()
    {
        when(consentConsumer.isRunningOnInstanceConnectedToPrimaryDB()).thenReturn(false);
        when(consentConsumer.isListening()).thenReturn(false);
        doNothing().when(consentConsumer).forceToggle(true);

        assertThat("Should have been stopped", consentConsumerServiceMBean.stopConsumer(), is(false));
        verify(consentConsumer).forceToggle(true);
        verify(consentConsumer).isListening();
    }
}
