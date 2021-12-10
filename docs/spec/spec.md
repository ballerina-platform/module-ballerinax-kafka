# Specification: Ballerina Kafka Library

_Owners_: @shafreenAnfar @dilanSachi @aashikam    
_Reviewers_: @shafreenAnfar @aashikam  
_Created_: 2020/10/28   
_Updated_: 2021/12/07  
_Issue_: [#2186](https://github.com/ballerina-platform/ballerina-standard-library/issues/2186)  

# Introduction
This is the specification for Kafka standard library which is used to send and receive messages by connecting to a Kafka server.
This library is programmed in the [Ballerina programming language](https://ballerina.io/), which is an open-source programming language for the cloud
that makes it easier to use, combine, and create network services.

# Contents

1. [Overview](#1-overview)  
2. [Configurations](#2-configurations)  
   2.1 [Producer Configurations](#21-producer-configurations)   
   2.2 [Consumer/Listener Configurations](#22-consumerlistener-configurations)    
   2.3 [Security Configurations](#23-security-configurations)      
   2.4 [Common Configurations](#24-common-configurations)   
   *   2.4.1 [TopicPartition](#241-topicpartition)     
   *   2.4.2 [PartitionOffset](#242-partitionoffset)   
3. [Producer](#3-producer)  
   3.1 [Connection](#31-connection)     
   *    3.1.1 [Insecure Connection](#311-insecure-connection)   
   *    3.1.2 [Secure Connection](#312-secure-connection)   
   3.2 [Producer Usage](#32-producer-usage)
4. [Consumer](#4-consumer)  
   4.1 [Connection](#41-connection)  
   *    4.1.1 [Insecure Connection](#411-insecure-connection)
   *    4.1.2 [Secure Connection](#412-secure-connection)
   4.2 [Consumer Usage](#42-consumer-usage)    
   *    4.2.1 [Consume Messages](#421-consume-messages)
   *    4.2.2 [Handle Offsets](#422-handle-offsets)    
   *    4.2.3 [Handle Partitions](#423-handle-partitions)    
   *    4.2.4 [Seeking](#424-seeking)    
   *    4.2.5 [Handle subscriptions](#425-handle-subscriptions)   
5. [Listener](#5-listener)  
   5.1 [Connection](#51-connection)     
   *    5.1.1 [Insecure Connection](#511-insecure-connection)
   *    5.1.2 [Secure Connection](#512-secure-connection)
   5.2 [Listener Usage](#52-listener-usage)    
   *    5.2.1 [Caller](#521-caller)
6. [Samples](#6-samples)
   6.1 [Produce Messages](#61-produce-messages)
   6.2 [Consume Messages](#62-consume-messages)
   *    6.2.1 [Using Consumer](#621-using-consumer)
   *    6.2.2 [Using Listener](#622-using-listener)

## 1. Overview

Apache Kafka is an open-source distributed event streaming platform for high-performance data pipelines, 
streaming analytics, data integration, and mission-critical applications. This specification elaborates on 
the usage of Kafka clients, producer and consumer. These clients allow writing distributed applications and 
microservices that read, write, and process streams of events in parallel, at scale, and in a fault-tolerant 
manner even in the case of network problems or machine failures.

Ballerina Kafka contains three core apis:
1. Producer
2. Consumer
3. Listener

## 2. Configurations
### 2.1 Producer Configurations
When initializing the producer, following configurations can be provided.
```ballerina
public type ProducerConfiguration record {|
    ProducerAcks acks = ACKS_SINGLE;
    CompressionType compressionType = COMPRESSION_NONE;
    string clientId?;
    string metricsRecordingLevel?;
    string metricReporterClasses?;
    string partitionerClass?;
    string interceptorClasses?;
    string transactionalId?;
    string schemaRegistryUrl?;
    map<string> additionalProperties?;
    int bufferMemory?;
    int retryCount?;
    int batchSize?;
    decimal linger?;
    int sendBuffer?;
    int receiveBuffer?;
    int maxRequestSize?;
    decimal reconnectBackoffTime?;
    decimal reconnectBackoffMaxTime?;
    decimal retryBackoffTime?;
    decimal maxBlock?;
    decimal requestTimeout?;
    decimal metadataMaxAge?;
    decimal metricsSampleWindow?;
    int metricsNumSamples?;
    int maxInFlightRequestsPerConnection?;
    decimal connectionsMaxIdleTime?;
    decimal transactionTimeout?;
    boolean enableIdempotence = false;
    SecureSocket secureSocket?;
    AuthenticationConfiguration auth?;
    SecurityProtocol securityProtocol = PROTOCOL_PLAINTEXT;
|};
```
A `ProducerRecord` corresponds to a message and other metadata that is sent to the Kafka server.
```ballerina
public type ProducerRecord record {|
    string topic;
    byte[] key?;
    byte[] value;
    int timestamp?;
    int partition?;
|};
```
### 2.2 Consumer/Listener Configurations
When initializing the consumer or the listener, following configurations can be provided.
```ballerina
public type ConsumerConfiguration record {|
    string groupId?;
    string[] topics?;
    OffsetResetMethod offsetReset?;
    string partitionAssignmentStrategy?;
    string metricsRecordingLevel?;
    string metricsReporterClasses?;
    string clientId?;
    string interceptorClasses?;
    IsolationLevel isolationLevel?;
    string schemaRegistryUrl?;
    map<string> additionalProperties?;
    decimal sessionTimeout?;
    decimal heartBeatInterval?;
    decimal metadataMaxAge?;
    decimal autoCommitInterval?;
    int maxPartitionFetchBytes?;
    int sendBuffer?;
    int receiveBuffer?;
    int fetchMinBytes?;
    int fetchMaxBytes?;
    decimal fetchMaxWaitTime?;
    decimal reconnectBackoffTimeMax?;
    decimal retryBackoff?;
    decimal metricsSampleWindow?;
    int metricsNumSamples?;
    decimal requestTimeout?;
    decimal connectionMaxIdleTime?;
    int maxPollRecords?;
    int maxPollInterval?;
    decimal reconnectBackoffTime?;
    decimal pollingTimeout?;
    decimal pollingInterval?;
    int concurrentConsumers?;
    decimal defaultApiTimeout?;
    boolean autoCommit = true;
    boolean checkCRCS = true;
    boolean excludeInternalTopics = true;
    boolean decoupleProcessing = false;
    SecureSocket secureSocket?;
    AuthenticationConfiguration auth?;
    SecurityProtocol securityProtocol = PROTOCOL_PLAINTEXT;
|};
```
A `ConsumerRecord` corresponds to a message and other metadata that is received from the Kafka server.
```ballerina
public type ConsumerRecord record {|
    byte[] key?;
    byte[] value;
    int timestamp;
    PartitionOffset offset;
|};
```
### 2.3 Security Configurations
- CertKey record represents the combination of certificate, private key and private key password if it is encrypted.
```ballerina
public type CertKey record {|
    string certFile;
    string keyFile;
    string keyPassword?;
|};
```
- Authentication record represents the Kafka authentication mechanism configurations.
```ballerina
public type AuthenticationConfiguration record {|
    AuthenticationMechanism mechanism = AUTH_SASL_PLAIN;
    string username;
    string password;
|};
```
- SecureSocket record represents the configurations needed for secure communication with the Kafka server.
```ballerina
public type SecureSocket record {|
   crypto:TrustStore|string cert;
   record {|
        crypto:KeyStore keyStore;
        string keyPassword?;
  |}|CertKey key?;
   record {|
        Protocol name;
        string[] versions?;
   |} protocol?;
   string[] ciphers?;
   string provider?;
|};
```
### 2.4 Common Configurations
There are some configurations which are common to Producer, Consumer and Listener.
#### 2.4.1 TopicPartition
This record represents a topic partition. A topic partition is the smallest storage unit that holds a subset of 
records owned by a topic.
```ballerina
public type TopicPartition record {|
    string topic;
    int partition;
|};
```
#### 2.4.2 PartitionOffset
This represents the topic partition position in which the consumed record is stored.
```ballerina
public type PartitionOffset record {|
    TopicPartition partition;
    int offset;
|};
```
## 3. Producer
The Producer API allows applications to send streams of data to topics in the Kafka cluster.
### 3.1 Connection
Connection with the Kafka server can be established insecurely or securely. By default, Kafka communicates in
`PLAINTEXT`, which means that all data is sent in the clear. It is recommended to communicate securely, though this
may have a performance impact due to encryption overhead.

#### 3.1.1 Insecure Connection
A simple insecure connection with the Kafka server can be easily established as follows.
```ballerina
kafka:Producer producer = check new (kafka:DEFAULT_URL);
```
Here, `kafka:DEFAULT_URL` has the value `localhost:9092`. The default producer configurations can be changed according 
to the user's need and pass as an argument when initializing the producer.
```ballerina
kafka:ProducerConfiguration producerConfiguration = {
    clientId: "simple-producer",
    acks: kafka:ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3
};
kafka:Producer producer = check new ("localhost:9190", producerConfiguration);
```
#### 3.1.2 Secure Connection
A secure connection with the Kafka server can be established via SSL as follows using either a `Truststore` or a certificate 
file. Additionally, a `Keystore` or a key file can also be provided. 
```ballerina
// Using TrustStore and KeyStore
crypto:TrustStore trustStore = {
    path: "../resource/path/to/truststore",
    password: "truststore password"
};

crypto:KeyStore keyStore = {
    path: "../resource/path/to/keystore",
    password: "keystore password"
};

kafka:SecureSocket socket = {
    cert: trustStore,
    key: {
        keyStore: keyStore,
        keyPassword: SSL_MASTER_PASSWORD
    },
    protocol: {
        name: kafka:SSL
    }
};

// Using certificate file and CertKey
kafka:CertKey certKey = {
    certFile: "../resource/path/to/certFile",
    keyFile: "../resource/path/to/keyFile"
};

kafka:SecureSocket socket = {
    cert: "../resource/path/to/certFile",
    key: certKey,
    protocol: {
        name: kafka:SSL
    }
};
// Provide secureSocket configuration to ProducerConfiguration
kafka:ProducerConfiguration producerConfiguration = {
    clientId: "secure-producer",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
// SSL_URL refers to the secure endpoint of the Kafka server
Producer producer = check new (SSL_URL, producerConfiguration);
```
In above, SSL encryption already enables 1-way authentication in which the client authenticates the server certificate. 
2-way authentication can be achieved using SASL authentication as follows.
```ballerina
kafka:AuthenticationConfiguration authConfig = {
    mechanism: kafka:AUTH_SASL_PLAIN,
    username: "username",
    password: "password"
};

kafka:ProducerConfiguration producerConfigs = {
    clientId: "secure-producer",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
### 3.2 Producer Usage
Kafka Producer API can be used to send messages to the Kafka server as follows.
```ballerina
string message = "Hello World, Ballerina";
kafka:Error? result = producer->send({topic: "kafka-topic", value: message.toBytes()});
// Closes the producer
kafka:Error? result = producer->close();
```
To make all the buffered records available to send, `flush()` API can be used after sending a message.
```ballerina
kafka:Error? result = producer->'flush();
```
When it is needed to send a message to a specific partition, `getTopicPartitions()` can be used to fetch the partitions 
of a specific topic.
```ballerina
kafka:TopicPartition[] result = check producer->getTopicPartitions("kafka-topic");
```
## 4. Consumer
The Consumer API allows applications to read streams of data from topics in the Kafka cluster.
### 4.1 Connection
Connection with the Kafka server can be established insecurely or securely as same as the producer.
#### 4.1.1 Insecure Connection
A simple insecure connection with the Kafka server can be easily established as follows.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "consumer-group",
    topics: ["kafka-topic"]
};
kafka:Consumer consumer = check new(kafka:DEFAULT_URL, consumerConfiguration);
```
Here, `kafka:DEFAULT_URL` has the value `localhost:9092`. The default consumer configurations can be changed according
to the user's need and pass as an argument when initializing the consumer.
```ballerina
ConsumerConfiguration consumerConfiguration = {
    topics: ["kafka-topic"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    groupId: "consumer-group",
    clientId: "simple-consumer",
    autoCommit: false
};
kafka:Consumer producer = check new ("localhost:9190", consumerConfiguration);
```
#### 4.1.2 Secure Connection
A secure connection with the Kafka server can be established via SSL as same as the Kafka Producer as follows using 
either a `Truststore` or a certificate file. Additionally, a `Keystore` or a key file can also be provided.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    topics: ["kafka-topic"],
    groupId: "consumer-group",
    clientId: "secure-consumer",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
// SSL_URL refers to the secure endpoint of the Kafka server
Consumer consumer = check new (SSL_URL, consumerConfiguration);
```
Consumer can be authenticated with the Kafka server using SASL as follows.
```ballerina
kafka:AuthenticationConfiguration authConfig = {
    mechanism: kafka:AUTH_SASL_PLAIN,
    username: "username",
    password: "password"
};

kafka:ConsumerConfiguration consumerConfiguration = {
    clientId: "secure-consumer",
    groupId: "consumer-group",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
### 4.2 Consumer Usage
#### 4.2.1 Consume Messages
Kafka consumer can consume messages by using the `poll()` method.
```ballerina
kafka:ConsumerRecord[] consumerRecords = check consumer->poll(5);
```
After consuming messages, the currently consumed offsets can be committed using `commit()`.
```ballerina
kafka:Error? result = consumer->commit();
```
Consumer can be closed after its usage using `close()` method.
```ballerina
kafka:Error? result = consumer->close();
```
#### 4.2.2 Handle Offsets
If it is needed to commit up to a specific offset, `commitOffset()` can be used.
```ballerina
TopicPartition topicPartition = {
    topic: "kafka-topic",
    partition: 0
};
PartitionOffset partitionOffset = {
    partition: topicPartition,
    offset: 1
};
kafka:Error? result = consumer->commitOffset([partitionOffset]);
```
`getBeginningOffsets()` retrieves the start offsets for a given set of partitions.
```ballerina
kafka:PartitionOffset[] partitionOffsets = check consumer->getBeginningOffsets([topicPartition]);
```
Last committed offsets can be retrieved using `getCommittedOffset()`.
```ballerina
kafka:PartitionOffset result = check consumer->getCommittedOffset(topicPartition);
```
The last offset can be retrieved using `getEndOffsets()`.
```ballerina
kafka:PartitionOffset[] result = check consumer->getEndOffsets([topicPartition]);
```
To retrieve the offset of the next record that will be fetched if a record exists in that position, `getPositionOffset()` 
can be used.
```ballerina
int result = check consumer->getPositionOffset(topicPartition);
```
#### 4.2.3 Handle Partitions
To assign a consumer to a set of topic partitions, `assign()` can be used.
```ballerina
kafka:Error? result = consumer->assign([topicPartition]);
```
To check the assigned topic partitions for a specific consumer, `getAssignment()` can be used.
```ballerina
kafka:TopicPartition[] result = check consumer->getAssignment();
```
To pause the message retrieval from a specific set of paritions, `pause()` can be used.
```ballerina
kafka:Error? result = consumer->pause([topicPartition]);
```
To resume the message retrieval from paused partitions, `resume()` can be used.
```ballerina
kafka:Error? result = consumer->resume([topicPartition]);
```
To get a list of the partitions that are currently paused from message retrieval, `getPausedPartitions()` can be used.
```ballerina
kafka:TopicPartition[] result = check consumer->getPausedPartitions();
```
To get the partitions to which a topic belong, `getTopicPartitions()` can be used.
```ballerina
kafka:TopicPartition[] result = check consumer->getTopicPartitions("kafka-topic");
```
#### 4.2.4 Seeking
To seek to a given offset in a topic partition, `seek()` can be used.
```ballerina
kafka:Error? result = consumer->seek(partitionOffset);
```
`seekToBeginning()` can be used to seek to the beginning of the offsets for a given set of topic partitions.
```ballerina
kafka:Error? result = consumer->seekToBeginning([topicPartition]);
```
`()` can be used to seek to the end of the offsets for a given set of topic partitions.
```ballerina
kafka:Error? result = consumer->seekToEnd([topicPartition]);
```
#### 4.2.5 Handle subscriptions
To get the topics that the consumer is currently subscribed, `getSubscription()` can be used.
```ballerina
string[] result = check consumer->getSubscription();
```
To get the list of topics currently available (authorized) for the consumer to subscribe, `getAvailableTopics()` can be 
used.
```ballerina
string[] topics = check consumer->getAvailableTopics();
```
To subscribe to a set of topics, `subscribe()` can be used.
```ballerina
kafka:Error? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
```
To subscribe to a set of topics that match a specific patter, `subscribeWithPattern()` can be used.
```ballerina
kafka:Error? result = consumer->subscribeWithPattern("kafka.*");
```
To unsubscribe from all the topics that the consumer is subscribed to, `unsubscribe()` can be used.
```ballerina
kafka:Error? result = consumer->unsubscribe();
```
## 5. Listener
### 5.1 Connection
A listener can be initialized as same as the consumer is initialized. The same configurations are used for the listener 
as well.
#### 5.1.1 Insecure Connection
A simple insecure connection with the Kafka server can be easily established as follows.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "consumer-group",
    topics: ["kafka-topic"]
};
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfiguration);
```
Just as the consumer and producer, the configurations can be changed according to the user's need.
#### 5.1.2 Secure Connection
A secure connection configurations are the same for the listener as in consumer.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    clientId: "secure-listener",
    groupId: "listener-group",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
### 5.2 Listener Usage
After initializing the listener, a service must be attached to the listener. There are two ways for this. 
1. Attach the service to the listener directly.
```ballerina
service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        // process results
    }
}
```
2. Attach the service dynamically.
```ballerina
// Create a service object
kafka:Service listenerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        // process results
    }
};
```
```ballerina
// Attach the service
error? result = kafkaListener.attach(listenerService);
// Start the listener to receive messages
error? result = kafkaListener.'start();
```
The remote function `onConsumerRecord()` is called when the listener receives messages from the Kafka server. If the 
`autoCommit` configuration of the listener is `false`, the consumed offsets will not be committed. In order to manually 
control this, the Caller API can be used.
#### 5.2.1 Caller
To commit the consumed offsets, `commit()` can be used.
```ballerina
kafka:Error? result = caller->commit();
```
To commit a specific offset, `commitOffset()` can be used.
```ballerina
kafka:Error? result = caller->commitOffset([partitionoffset]);
```
Apart from above, the listener has following functions to manage a service.
* `detach()` - can be used to detach a service from the listener.
```ballerina
error? result = kafkaListener.detach(listenerService);
```
* `gracefulStop()` - can be used to gracefully stop the listener from consuming messages.
```ballerina
error? result = kafkaListener.gracefulStop();
```
* `immediateStop()` - can be used to immediately stop the listener from consuming messages.
```ballerina
error? result = kafkaListener.immediateStop();
```
## 6. Samples
### 6.1 Produce Messages
```ballerina
import ballerinax/kafka;
import ballerina/crypto;

kafka:AuthenticationConfiguration authConfig = {
   mechanism: kafka:AUTH_SASL_PLAIN,
   username: "user",
   password: "password"
};

crypto:TrustStore trustStore = {
   path: "path/to/truststore",
   password: "password"
};

crypto:KeyStore keyStore = {
   path: "path/to/keystore",
   password: "password"
};

kafka:SecureSocket socket = {
   cert: trustStore,
   key: {
      keyStore: keyStore,
      keyPassword: "password"
   },
   protocol: {
      name: kafka:SSL
   }
};

kafka:ProducerConfiguration producerConfigs = {
   clientId: "kafka-producer",
   acks: kafka:ACKS_ALL,
   maxBlock: 6,
   requestTimeout: 2,
   retryCount: 3,
   auth: authConfig,
   secureSocket: socket,
   securityProtocol: kafka:PROTOCOL_SSL
};
 
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfigs);

public function main() returns error? {
    string message = "Hello World, Ballerina";
    check kafkaProducer->send({ topic: "kafka-topic", value: message.toBytes() });
    check kafkaProducer->'flush();
    check kafkaProducer->close();
}
```
### 6.2 Consume Messages
#### 6.2.1 Using Consumer
```ballerina
import ballerina/crypto;
import ballerina/io;
import ballerinax/kafka;

kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "consumer-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};

kafka:Consumer consumer = check new (kafka:DEFAULT_URL, consumerConfiguration);

public function main() returns error? {
    check consumer->subscribe(["kafka-topic"]);
    kafka:ConsumerRecord[] records = check consumer->poll(1);

    foreach var kafkaRecord in records {
        byte[] messageContent = kafkaRecord.value;
        string message = check string:fromBytes(messageContent);

        io:println("Received Message: " + message);
    }
}
```
#### 6.2.2 Using Listener
```ballerina
import ballerina/crypto;
import ballerina/log;
import ballerina/lang.runtime;
import ballerinax/kafka;

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "listener-group",
    topics: ["kafka-topic"],
    pollingInterval: 1,
    autoCommit: false,
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

public function main() returns error? {
    check kafkaListener.attach(listenerService);
    check kafkaListener.'start();
    runtime:registerListener(kafkaListener);
}

kafka:Service listenerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer ", 'error = commitResult);
        }
    }
};

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) returns error? {
    byte[] value = kafkaRecord.value;
    string messageContent = check string:fromBytes(value);
    log:printInfo("Received Message: " + messageContent);
}
```
