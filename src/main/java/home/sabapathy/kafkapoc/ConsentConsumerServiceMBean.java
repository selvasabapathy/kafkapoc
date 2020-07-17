package home.sabapathy.kafkapoc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.stereotype.Component;

@Component
public class ConsentConsumerServiceMBean implements ConsentConsumeService
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ConsentConsumerServiceMBean.class);

    @Autowired
    ConsentConsumer consentConsumer;

    @Override
    @ManagedOperation(description = "Start Consumer to listen to CDC to sync DGST")
    public boolean isConsumerListening()
    {
        boolean isRunning = consentConsumer.isListening();
        
        LOGGER.info("Listener running? {}", isRunning);
        return isRunning;
    }

    @Override
    @ManagedOperation(description = "Start Consumer to listen to CDC to sync DGST")
    public boolean startConsumer()
    {
        LOGGER.info("Force starting consumer");
        consentConsumer.forceToggle(false);
        return consentConsumer.isListening();
    }

    @Override
    @ManagedOperation(description = "Stop Consumer from listening to CDC")
    public boolean stopConsumer()
    {
        LOGGER.info("Force stopping consumer");
        consentConsumer.forceToggle(true);
        return consentConsumer.isListening();
    }

}
