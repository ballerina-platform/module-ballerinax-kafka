# Specification: Ballerina RabbitMQ Library

_Owners_: @shafreenAnfar @dilanSachi @aashikam    
_Reviewers_: @shafreenAnfar  
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
3. [Producer](#3-producer)
4. [Consumer](#4-consumer)
5. [Listener](#5-listener)

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
### 2.4 Security Configurations
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

## 2. Producer
The Producer API allows applications to send streams of data to topics in the Kafka cluster.
### 2.1 Connection
Connection with the Kafka server can be established insecurely or securely. By default, Kafka communicates in
`PLAINTEXT`, which means that all data is sent in the clear. It is recommended to communicate securely, though this
may have a performance impact due to encryption overhead.

#### 2.1.1 Insecure Connection
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
#### 2.1.2 Secure Connection
A secure connection with the Kafka server can be established via SSL as follows using either a `Truststore` or a certificate 
file. Additionally, a `Keystore` or a key file can also be provided. 
```ballerina
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

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "secure-producer",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
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
### 2.2 Produce Messages

## 3. Consumer
The Consumer API allows applications to read streams of data from topics in the Kafka cluster.
### 3.1 Configurations
### 3.2 Connection
#### 3.2.1 Insecure Connection

#### 3.2.2 Secure Connection

### 3.3 Consume Messages

## 4. Listener
### 4.1 Configurations
### 4.2 Connection
#### 4.2.1 Insecure Connection

#### 4.2.2 Secure Connection

### 4.3 Consumer Messages

