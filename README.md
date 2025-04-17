# nifi-azure-servicebus
Azure Service Bus Processors (Receive and Send) for Apache Nifi

## About
This project contains Azure Service Bus receivers and senders for Apache Nifi (2).

Nifi already provides a set of AMQP processors, however, these processors are currently incompatible with Azure Service Bus because they taget AMQP 0.9 and Service Bus implements AMQP 1.0.

This set of processors employs the Azure Service Bus SDK for both receive and send work.

## Tooling/Build
This project was built using Visual Studio Code and OpenJDK 21.  To build, open the src/nifi-azure-servicebus directory and run mvn clean compile.

## Deployment
Build and package from the src/nifi-azure-servicebus directory using mvn clean install.  This will produce the nar package in src/nifi-azure-servicebus-nifi-nifiAzureServiceBus-nar/target.  Copy this nar file to your Nifi installation's lib folder (and restart Nifi).

## Disclaimer
Use at your own risk.  Limited testing performed.  No warranty provided.
