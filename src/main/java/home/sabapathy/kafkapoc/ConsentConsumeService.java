package home.sabapathy.kafkapoc;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(objectName = "IHG:name=/br/consent/consumer", description = "JMX wrapper for BR Consent Consumer")
public interface ConsentConsumeService
{
    @ManagedOperation(description = "Start Consumer to listen to CDC to sync DGST")
    boolean isConsumerListening();

    @ManagedOperation(description = "Start Consumer to listen to CDC to sync DGST")
    boolean startConsumer();

    @ManagedOperation(description = "Stop Consumer from listening to CDC")
    boolean stopConsumer();
}
