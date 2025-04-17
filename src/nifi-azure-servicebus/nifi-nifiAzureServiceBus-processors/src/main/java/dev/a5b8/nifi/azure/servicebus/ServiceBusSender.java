/*
 * This processor was greatly influenced by the following Apache Nifi source code:
 * https://github.com/apache/nifi/blob/main/nifi-extension-bundles/nifi-amqp-bundle/nifi-amqp-processors/src/main/java/org/apache/nifi/amqp/processors/AbstractAMQPProcessor.java
 * 
 */

package dev.a5b8.nifi.azure.servicebus;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.azure.messaging.servicebus.*;

@Tags({"azure", "service bus", "send", "queue"})
@CapabilityDescription("Sends messages to an Azure Service Bus queue.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ServiceBusSender extends AbstractProcessor {

    // Properties

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor
            .Builder()
            .name("Connection String")
            .displayName("Connection String")
            .description("The connection string to the Azure Service Bus")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUEUE_NAME = new PropertyDescriptor
            .Builder()
            .name("Queue Name")
            .displayName("Queue Name")
            .description("The name of the queue to send messages to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();        

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully sent to Service Bus.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to send to Service Bus.")
            .build();

    // Methods

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(
            CONNECTION_STRING,
            QUEUE_NAME
        );

        relationships = Set.of(
            REL_SUCCESS,
            REL_FAILURE
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        connectionString = context.getProperty(CONNECTION_STRING).getValue();
        queueName = context.getProperty(QUEUE_NAME).getValue();
        senderQueue = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());  
        
        for (int i = 0; i < context.getMaxConcurrentTasks(); i++) {
            ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();                
            senderQueue.offer(senderClient);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = null;
        ServiceBusSenderClient senderClient = null;
        try {
            flowFile = session.get();
            if (flowFile == null) {
                return;
            }
            
            senderClient = senderQueue.poll();
            if (senderClient == null) {
                throw new RuntimeException("Unable to create or obtain a Service Bus receiver client.");
            }
            
            String messageBody;
            try (InputStream inputStream = session.read(flowFile)) {
                messageBody = new String (inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }

            ServiceBusMessage message = new ServiceBusMessage(messageBody);
            senderClient.sendMessage(message);  
            session.transfer(flowFile, REL_SUCCESS);

            getLogger().info("Message sent to Service Bus: {}", messageBody);  
        }
        catch (Exception e) {
            getLogger().error("Failed to send message to Service Bus. Error: {}", e);

            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }            
        } finally {
            if (senderClient != null) {
                senderQueue.offer(senderClient);
            }
        }        
    }

    @OnStopped
    public void onStopped() {
        if (senderQueue != null) {
            while (!senderQueue.isEmpty()) {
                ServiceBusSenderClient senderClient = senderQueue.poll();
                if (senderClient != null) {
                    senderClient.close();
                }
            }
        }
    }

    // Fields

    private String connectionString;
    private String queueName;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private BlockingQueue<ServiceBusSenderClient> senderQueue;
}
