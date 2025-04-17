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
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;
import java.io.OutputStream;
import java.lang.module.FindException;
import java.nio.charset.StandardCharsets;
import java.security.Provider.Service;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.azure.messaging.servicebus.*;

@Tags({"azure", "service bus", "send", "queue"})
@CapabilityDescription("Receives messages from an Azure Service Bus queue.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ServiceBusReceiver extends AbstractProcessor {

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

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("The number of messages to receive in a single batch.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();    

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Timeout")
            .displayName("Timeout")
            .description("The maximum time (milliseconds) to wait for messages.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("5000")
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
            QUEUE_NAME,
            BATCH_SIZE,
            TIMEOUT);

        relationships = Set.of(
            REL_SUCCESS, 
            REL_FAILURE);
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
        batchSize = context.getProperty(BATCH_SIZE).asInteger();
        timeout = Duration.ofMillis(context.getProperty(TIMEOUT).asLong());
        receiverQueue = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        ServiceBusReceiverClient receiverClient = null;
        List<ServiceBusReceivedMessage> messages = new ArrayList<>();
        boolean success = false;

        try {
            receiverClient = receiverQueue.poll();
            if (receiverClient == null) {
                receiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
                .buildClient();                
            }

            if (receiverClient == null) {
                throw new RuntimeException("Unable to create or obtain a Service Bus receiver client.");
            }
            
            for (ServiceBusReceivedMessage message : receiverClient.receiveMessages(batchSize, timeout)) {
                messages.add(message);
            }

            List<FlowFile> flowFiles = new ArrayList<>();
            for (ServiceBusReceivedMessage message : messages) {
                FlowFile flowFile = session.create();
                flowFile = session.putAttribute(flowFile, "messageId", message.getMessageId());
                try (OutputStream out = session.write(flowFile)) {
                    out.write(message.getBody().toBytes());
                }
                flowFiles.add(flowFile);            
            }
    
            if (!flowFiles.isEmpty()) {
                session.transfer(flowFiles, REL_SUCCESS);
                for (ServiceBusReceivedMessage message : messages) {
                    receiverClient.complete(message);
                }
                success = true;
            }                

        } catch(Exception e) {
            getLogger().error("Failed to receive messages from Service Bus: {}", e);
            for (ServiceBusReceivedMessage message : messages) {
                try { 
                    receiverClient.abandon(message); 
                } catch (Exception ex) { 
                    getLogger().warn("Failed to abandon message: {}", ex);
                }
            }
        } finally {
            if (receiverClient != null) {
                receiverQueue.offer(receiverClient);
            }
            if (!success) {
                context.yield();
            }
        } 
    }

    @OnStopped
    public void onStopped() {
        if (receiverQueue != null) {
            while (!receiverQueue.isEmpty()) {
                ServiceBusReceiverClient receiverClient = receiverQueue.poll();
                if (receiverClient != null) {
                    receiverClient.close();
                }
            }
        }
    }

    // Fields

    private String connectionString;
    private String queueName;
    private int batchSize;
    private Duration timeout;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private BlockingQueue<ServiceBusReceiverClient> receiverQueue;    
}
