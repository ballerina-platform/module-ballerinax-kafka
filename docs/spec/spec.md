# Specification: Ballerina Kafka Library

_Owners_: @shafreenAnfar @dilanSachi @aashikam    
_Reviewers_: @shafreenAnfar @aashikam  
_Created_: 2020/10/28   
_Updated_: 2022/05/11   
_Edition_: Swan Lake  

# Introduction
This is the specification for the Kafka standard library of [Ballerina language](https://ballerina.io/), which can send and receive messages by connecting to a Kafka server.

The Kafka library specification has evolved and may continue to evolve in the future. The released versions of the specification can be found under the relevant GitHub tag.

If you have any feedback or suggestions about the library, start a discussion via a [GitHub issue](https://github.com/ballerina-platform/ballerina-standard-library/issues) or in the [Slack channel](https://ballerina.io/community/). Based on the outcome of the discussion, the specification and implementation can be updated. Community feedback is always welcome. Any accepted proposal, which affects the specification is stored under `/docs/proposals`. Proposals under discussion can be found with the label `type/proposal` in GitHub.

The conforming implementation of the specification is released to Ballerina central. Any deviation from the specification is considered a bug.

# Contents
1. [Overview](#1-overview)
2. [Configurations](#2-configurations)
   *  2.1. [Security Configurations](#21-security-configurations)
   *  2.2. [TopicPartition](#22-topicpartition)
   *  2.3. [PartitionOffset](#23-partitionoffset)
3. [Producer](#3-producer)
   *  3.1. [Configurations](#31-configurations)
   *  3.2. [Initialization](#32-initialization)
      *    3.2.1. [Insecure Client](#321-insecure-client)
      *    3.2.2. [Secure Client](#322-secure-client)
   *  3.3. [Functions](#33-functions)
4. [Consumer](#4-consumer)
   *  4.1. [Configurations](#41-configurations)
   *  4.2. [Consumer Client](#42-consumer-client)
      *  4.2.1. [Initialization](#421-initialization)
         *  4.2.1.1. [Insecure Client](#4211-insecure-client)
         *  4.2.1.2. [Secure Client](#4212-secure-client)
      *  4.2.2. [Consume Messages](#422-consume-messages)
      *  4.2.3. [Handle Offsets](#423-handle-offsets)
      *  4.2.4. [Handle Partitions](#424-handle-partitions)
      *  4.2.5. [Seeking](#425-seeking)
      *  4.2.6. [Handle subscriptions](#426-handle-subscriptions)
   *  4.3. [Listener](#43-listener)
      *  4.3.1. [Initialization](#431-initialization)
         *  4.3.1.1. [Insecure Listener](#4311-insecure-listener)
         *  4.3.1.2. [Secure Listener](#4312-secure-listener)
      *  4.3.2. [Usage](#432-usage)
      *  4.3.3. [Caller](#433-caller)
5. [Samples](#5-samples)
   *  5.1. [Produce Messages](#51-produce-messages)
   *  5.2. [Consume Messages](#52-consume-messages)
      *    5.2.1. [Using Consumer Client](#521-using-consumer-client)
      *    5.2.2. [Using Listener](#522-using-listener)

## 1. Overview
Apache Kafka is an open-source distributed event streaming platform for high-performance data pipelines, 
streaming analytics, data integration, and mission-critical applications. This specification elaborates on 
the usage of Kafka clients, producer and consumer. These clients allow writing distributed applications and 
microservices that read, write, and process streams of events in parallel, at scale, and in a fault-tolerant 
manner even in the case of network problems or machine failures.

Ballerina Kafka contains three core apis:
* Producer - Used to publish messages to the Kafka server.
* Consumer & Listener - Used to get the messages from the Kafka server.

## 2. Configurations
### 2.1. Security Configurations
* CertKey record represents the combination of certificate, private key and private key password if it is encrypted.
```ballerina
public type CertKey record {|
    # A file containing the certificate
    string certFile;
    # A file containing the private key in PKCS8 format
    string keyFile;
    # Password of the private key if it is encrypted
    string keyPassword?;
|};
```
* Authentication record represents the Kafka authentication mechanism configurations.
```ballerina
public type AuthenticationConfiguration record {|
    # Type of the authentication mechanism. Currently, SASL_PLAIN and SCRAM are supported
    AuthenticationMechanism mechanism = AUTH_SASL_PLAIN;
    # The username to authenticate the Kafka producer/consumer
    string username;
    # The password to authenticate the Kafka producer/consumer
    string password;
|};
```
* SecureSocket record represents the configurations needed for secure communication with the Kafka server.
```ballerina
public type SecureSocket record {|
   # Configurations associated with crypto:TrustStore or single certificate file that the client trusts
   crypto:TrustStore|string cert;
   # Configurations associated with crypto:KeyStore or combination of certificate and private key of the client
   record {|
      crypto:KeyStore keyStore;
      string keyPassword?;
   |}|CertKey key?;
   # SSL/TLS protocol related options
   record {|
      Protocol name;
      string[] versions?;
   |} protocol?;
   # List of ciphers to be used. By default, all the available cipher suites are supported
   string[] ciphers?;
   # Name of the security provider used for SSL connections. The default value is the default security provider
   # of the JVM
   string provider?;
|};
```
### 2.2. TopicPartition
* This record represents a topic partition. A topic partition is the smallest storage unit that holds a subset of 
records owned by a topic.
```ballerina
public type TopicPartition record {|
    # Topic to which the partition is related
    string topic;
    # Index of the specific partition
    int partition;
|};
```
### 2.3. PartitionOffset
* This represents the topic partition position in which the consumed record is stored.
```ballerina
public type PartitionOffset record {|
    # The `kafka:TopicPartition` to which the record is related
    TopicPartition partition;
    # Offset in which the record is stored in the partition
    int offset;
|};
```
## 3. Producer
The Producer client allows applications to send streams of data to topics in the Kafka cluster.
Connection with the Kafka server can be established insecurely or securely. By default, Kafka communicates in
`PLAINTEXT`, which means that all data is sent in the clear. It is recommended to communicate securely, though this
may have a performance impact due to encryption overhead.
### 3.1. Configurations
* When initializing the producer, following configurations can be provided.
```ballerina
public type ProducerConfiguration record {|
    # Number of acknowledgments
    ProducerAcks acks = ACKS_SINGLE;
    # Compression type to be used for messages
    CompressionType compressionType = COMPRESSION_NONE;
    # Identifier to be used for server side logging
    string clientId?;
    # Metrics recording level
    string metricsRecordingLevel?;
    # Metrics reporter classes
    string metricReporterClasses?;
    # Partitioner class to be used to select the partition to which the message is sent
    string partitionerClass?;
    # Interceptor classes to be used before sending the records
    string interceptorClasses?;
    # Transactional ID to be used in transactional delivery
    string transactionalId?;
    # Avro schema registry URL. Use this field to specify the schema registry URL if the Avro serializer is used
    string schemaRegistryUrl?;
    # Additional properties for the property fields not provided by the Ballerina `kafka` module. Use
    # this with caution since this can override any of the fields. It is not recomendded to use
    # this field except in an extreme situation
    map<string> additionalProperties?;
    # Total bytes of memory the producer can use to buffer records
    int bufferMemory?;
    # Number of retries to resend a record
    int retryCount?;
    # Maximum number of bytes to be batched together when sending the records. Records exceeding this limit will
    # not be batched. Setting this to 0 will disable batching
    int batchSize?;
    # Delay (in seconds) to allow other records to be batched before sending them to the Kafka server
    decimal linger?;
    # Size of the TCP send buffer (SO_SNDBUF)
    int sendBuffer?;
    # Size of the TCP receive buffer (SO_RCVBUF)
    int receiveBuffer?;
    # The maximum size of a request in bytes
    int maxRequestSize?;
    # Time (in seconds) to wait before attempting to reconnect
    decimal reconnectBackoffTime?;
    # Maximum amount of time in seconds to wait when reconnecting
    decimal reconnectBackoffMaxTime?;
    # Time (in seconds) to wait before attempting to retry a failed request
    decimal retryBackoffTime?;
    # Maximum block time (in seconds) during which the sending is blocked when the buffer is full
    decimal maxBlock?;
    # Wait time (in seconds) for the response of a request
    decimal requestTimeout?;
    # Maximum time (in seconds) to force a refresh of metadata
    decimal metadataMaxAge?;
    # Time (in seconds) window for a metrics sample to compute over
    decimal metricsSampleWindow?;
    # Number of samples maintained to compute the metrics
    int metricsNumSamples?;
    # Maximum number of unacknowledged requests on a single connection
    int maxInFlightRequestsPerConnection?;
    # Close the idle connections after this number of seconds
    decimal connectionsMaxIdleTime?;
    # Timeout (in seconds) for transaction status update from the producer
    decimal transactionTimeout?;
    # Exactly one copy of each message is written to the stream when enabled
    boolean enableIdempotence = false;
    # Configurations related to SSL/TLS encryption
    SecureSocket secureSocket?;
    # Authentication-related configurations for the Kafka producer
    AuthenticationConfiguration auth?;
    # Type of the security protocol to use in the broker connection
    SecurityProtocol securityProtocol = PROTOCOL_PLAINTEXT;
|};
```
* A `kafka:AnydataProducerRecord` corresponds to a message and other metadata that is sent to the Kafka server.
```ballerina
public type AnydataProducerRecord record {|
    # Topic to which the record will be appended
    string topic;
    # Key that is included in the record
    anydata key?;
    # Record content
    anydata value;
    # Timestamp of the record, in milliseconds since epoch
    int timestamp?;
    # Partition to which the record should be sent
    int partition?;
|};
```
* `kafka:BytesProducerRecord` defines the subtype of `kafka:AnydataProducerRecord` where the value is a `byte[]`;
```ballerina
public type BytesProducerRecord record {|
    *AnydataProducerRecord;
    // Record content in bytes
    byte[] value;
|};
```
### 3.2. Initialization
#### 3.2.1. Insecure Client
A simple insecure client can be initialized by providing the Kafka server url and the `ProducerConfiguration`.
```ballerina
# Creates a new Kafka `Producer`.
#
# + bootstrapServers - List of remote server endpoints of Kafka brokers
# + config - Configurations related to initializing a Kafka `Producer`
# + return - A `kafka:Error` if closing the producer failed or else '()'
public isolated function init(string|string[] bootstrapServers, *ProducerConfiguration config) returns Error?
```
#### 3.2.2. Secure Client
A secure Kafka client can be initialized by providing either a `crypto:Truststore` or a certificate file to the 
`ProducerConfiguration`. Additionally, a `crypto:Keystore` or a key file can also be provided. 
```ballerina
// Provide secureSocket configuration to ProducerConfiguration
kafka:ProducerConfiguration producerConfiguration = {
    clientId: "secure-producer",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
```
In above, SSL encryption already enables 1-way authentication in which the client authenticates the server certificate. 
2-way authentication can be achieved using SASL authentication by providing `kafka:AuthenticationConfiguration` to 
the `kafka:ProducerConfiguration`.
```ballerina
kafka:ProducerConfiguration producerConfigs = {
    clientId: "secure-producer",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
### 3.3. Functions
* Kafka Producer API can be used to send messages to the Kafka server. For this, the `send()` method can be used.
```ballerina
# Produces records to the Kafka server.
# ```ballerina
# kafka:Error? result = producer->send({value: "Hello World, Ballerina", topic: "kafka-topic"});
# ```
#
# + producerRecord - Record to be produced
# + return -  A `kafka:Error` if send action fails to send data or else '()'
isolated remote function send(AnydataProducerRecord producerRecord) returns Error?;
```
* If the user wants to ensure the message order or ensure that the message goes to a specific partition, `key` and
  `partition` configurations can be provided in the `ProducerRecord`;
* To close the producer after the usage, `close()` can be used.
```ballerina
# Closes the producer connection to the external Kafka broker.
# ```ballerina
# kafka:Error? result = producer->close();
# ```
#
# + return - A `kafka:Error` if closing the producer failed or else '()'
isolated remote function close() returns Error?;
```
* To make all the buffered records available to send, `flush()` API can be used after sending a message.
```ballerina
# Flushes the batch of records already sent to the broker by the producer.
# ```ballerina
# kafka:Error? result = producer->'flush();
# ```
#
# + return - A `kafka:Error` if records couldn't be flushed or else '()'
isolated remote function 'flush() returns Error?;
```
* `getTopicPartitions()` can be used to fetch the partitions of a specific topic.
```ballerina
# Retrieves the topic partition information for the provided topic.
# ```ballerina
# kafka:TopicPartition[] result = check producer->getTopicPartitions("kafka-topic");
# ```
#
# + topic - The specific topic, of which the topic partition information is required
# + return - A `kafka:TopicPartition` array for the given topic or else a `kafka:Error` if the operation fails
isolated remote function getTopicPartitions(string topic) returns TopicPartition[]|Error;
```
## 4. Consumer
The Consumer allows applications to read streams of data from topics in the Kafka cluster. Ballerina Kafka supports
two types of consumers, Consumer Client and Listener.
### 4.1. Configurations
* When initializing the consumer or the listener, following configurations can be provided.
```ballerina
public type ConsumerConfiguration record {|
    # Unique string that identifies the consumer
    string groupId?;
    # Topics to be subscribed by the consumer
    string[] topics?;
    # Offset reset strategy if no initial offset
    OffsetResetMethod offsetReset?;
    # Strategy class for handling the partition assignment among consumers
    string partitionAssignmentStrategy?;
    # Metrics recording level
    string metricsRecordingLevel?;
    # Metrics reporter classes
    string metricsReporterClasses?;
    # Identifier to be used for server side logging
    string clientId?;
    # Interceptor classes to be used before sending the records
    string interceptorClasses?;
    # Transactional message reading method
    IsolationLevel isolationLevel?;
    # Avro schema registry URL. Use this field to specify the schema registry URL, if the Avro serializer
    # is used
    string schemaRegistryUrl?;
    # Additional properties for the property fields not provided by the Ballerina `kafka` module. Use
    # this with caution since this can override any of the fields. It is not recomendded to use
    # this field except in an extreme situation
    map<string> additionalProperties?;
    # Timeout (in seconds) used to detect consumer failures when the heartbeat threshold is reached
    decimal sessionTimeout?;
    # Expected time (in seconds) between the heartbeats
    decimal heartBeatInterval?;
    # Maximum time (in seconds) to force a refresh of metadata
    decimal metadataMaxAge?;
    # Auto committing interval (in seconds) for commit offset when auto-committing is enabled
    decimal autoCommitInterval?;
    # The maximum amount of data the server returns per partition
    int maxPartitionFetchBytes?;
    # Size of the TCP send buffer (SO_SNDBUF)
    int sendBuffer?;
    # Size of the TCP receive buffer (SO_RCVBUF)
    int receiveBuffer?;
    # Minimum amount of data the server should return for a fetch request
    int fetchMinBytes?;
    # Maximum amount of data the server should return for a fetch request
    int fetchMaxBytes?;
    # Maximum amount of time (in seconds) the server will block before answering the fetch request
    decimal fetchMaxWaitTime?;
    # Maximum amount of time in seconds to wait when reconnecting
    decimal reconnectBackoffTimeMax?;
    # Time (in seconds) to wait before attempting to retry a failed request
    decimal retryBackoff?;
    # Window of time (in seconds) a metrics sample is computed over
    decimal metricsSampleWindow?;
    # Number of samples maintained to compute metrics
    int metricsNumSamples?;
    # Wait time (in seconds) for response of a request
    decimal requestTimeout?;
    # Close idle connections after the number of seconds
    decimal connectionMaxIdleTime?;
    # Maximum number of records returned in a single call to poll
    int maxPollRecords?;
    # Maximum delay between invocations of poll
    int maxPollInterval?;
    # Time (in seconds) to wait before attempting to reconnect
    decimal reconnectBackoffTime?;
    # Timeout interval for polling in seconds
    decimal pollingTimeout?;
    # Polling interval for the consumer in seconds
    decimal pollingInterval?;
    # Number of concurrent consumers
    int concurrentConsumers?;
    # Default API timeout value (in seconds) for APIs with duration
    decimal defaultApiTimeout?;
    # Enables auto committing offsets
    boolean autoCommit = true;
    # Check the CRC32 of the records consumed. This ensures that no on-the-wire or on-disk corruption to
    # the messages occurred. This may add some overhead, and might needed set to `false` if extreme
    # performance is required
    boolean checkCRCS = true;
    # Whether records from internal topics should be exposed to the consumer
    boolean excludeInternalTopics = true;
    # Decouples processing
    boolean decoupleProcessing = false;
    # Configurations related to SSL/TLS encryption
    SecureSocket secureSocket?;
    # Authentication-related configurations for the Kafka consumer
    AuthenticationConfiguration auth?;
    # Type of the security protocol to use in the broker connection
    SecurityProtocol securityProtocol = PROTOCOL_PLAINTEXT;
|};
```
* A `kafka:AnydataConsumerRecord` corresponds to a message and other metadata that is received from the Kafka server.
```ballerina
public type AnydataConsumerRecord record {|
    # Key that is included in the record
    anydata key?;
    # Record content
    anydata value;
    # Timestamp of the record, in milliseconds since epoch
    int timestamp;
    # Topic partition position in which the consumed record is stored
    PartitionOffset offset;
|};
```
* `kafka:BytesConsumerRecord` defines the subtype of `kafka:AnydataConsumerRecord` where the value is a `byte[]`.
```ballerina
public type BytesConsumerRecord record {|
    *AnydataConsumerRecord;
    // Record content in bytes
    byte[] value;
|};
```
### 4.2. Consumer Client
The Consumer Client provides the ability to pull messages from the Kafka server anytime the user needs and has 
APIs to manage subscriptions, topic partitions, offsets etc.
#### 4.2.1. Initialization
Connection with the Kafka server can be established insecurely or securely as same as the producer.
##### 4.2.1.1 Insecure Client
A simple insecure connection with the Kafka server can be easily established by providing the Kafka server URL and
basic configuration.
```ballerina
# Creates a new Kafka `Consumer`.
#
# + bootstrapServers - List of remote server endpoints of Kafka brokers
# + config - Configurations related to the consumer endpoint
# + return - A `kafka:Error` if an error is encountered or else '()'
public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error?;
```
##### 4.2.1.2. Secure Client
A secure client can be established via SSL as same as the Kafka Producer using either a `crypto:Truststore` or a
certificate file. Additionally, a `crypto:Keystore` or a key file can also be provided.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    topics: ["kafka-topic"],
    groupId: "consumer-group",
    clientId: "secure-consumer",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
```
Consumer client can be authenticated with the Kafka server using SASL by providing `kafka:AuthenticationConfiguration`.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    clientId: "secure-consumer",
    groupId: "consumer-group",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
#### 4.2.2. Consume Messages
* Kafka consumer can consume messages by using the `poll()` method.
```ballerina
# Polls the external broker to retrieve messages.
# ```ballerina
# kafka:AnydataConsumerRecord[] result = check consumer->poll(10);
# ```
#
# + timeout - Polling time in seconds
# + T - Optional type description of the required data type
# + return - Array of consumer records if executed successfully or else a `kafka:Error`
isolated remote function poll(decimal timeout, typedesc<AnydataConsumerRecord[]> T = <>) returns T|Error;
```
* When polling, a timeout value can be specified, and it will be the maximum time that the `poll()` method will block for.
* Subtypes of `kafka:AnydataConsumerRecord` can be created to bind the data to a specific type.
```ballerina
public type StringConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    string content;
|};
StringConsumerRecord[] stringRecords = check consumer->poll(10);
```
* If none of the metadata of the consumer records are not needed, `pollPayload` api can be used to directly get the payload in the intended type.
```ballerina
# Polls the external broker to retrieve messages in the required data type without the `kafka:AnydataConsumerRecord`
# information.
# ```ballerina
# Person[] persons = check consumer->pollPayload(10);
# ```
#
# + timeout - Polling time in seconds
# + T - Optional type description of the required data type
# + return - Array of data in the required format if executed successfully or else a `kafka:Error`
isolated remote function pollPayload(decimal timeout, typedesc<anydata[]> T = <>) returns T|Error;
```
* After consuming messages, the consumed offsets can be committed to the Kafka server. This can be done automatically by 
specifying `autoCommit: true` in `kafka:ConsumerConfiguration` or by manually using `commit()`.
```ballerina
# Commits the currently consumed offsets of the consumer.
# ```ballerina
# kafka:Error? result = consumer->commit();
# ```
#
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function 'commit() returns Error?;
```
* Consumer can be closed after its usage using `close()` method.
```ballerina
# Closes the consumer connection with the external Kafka broker.
# ```ballerina
# kafka:Error? result = consumer->close();
# ```
#
# + duration - Timeout duration (in seconds) for the close operation execution
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function close(decimal duration = -1) returns Error?;
```
#### 4.2.3. Handle Offsets
* If it is needed to commit up to a specific offset, `commitOffset()` can be used.
```ballerina
# Commits the given offsets of the specific topic partitions for the consumer.
# ```ballerina
# kafka:Error? result = consumer->commitOffset([partitionOffset]);
# ```
#
# + offsets - Offsets to be commited
# + duration - Timeout duration (in seconds) for the commit operation execution
# + return - `kafka:Error` if an error is encountered or else `()`
isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error?;
```
* `getBeginningOffsets()` retrieves the start offsets for a given set of partitions.
```ballerina
# Retrieves the start offsets for a given set of partitions.
# ```ballerina
# kafka:PartitionOffset[] result = check consumer->getBeginningOffsets([topicPartition1, topicPartition2]);
# ```
#
# + partitions - Array of topic partitions to get the starting offsets
# + duration - Timeout duration (in seconds) for the `getBeginningOffsets` execution
# + return - Starting offsets for the given partitions if executes successfully or else a `kafka:Error`
isolated remote function getBeginningOffsets(TopicPartition[] partitions, decimal duration = -1) returns PartitionOffset[]|Error;
```
* Last committed offsets can be retrieved using `getCommittedOffset()`.
```ballerina
# Retrieves the lastly committed offset for the given topic partition.
# ```ballerina
# kafka:PartitionOffset result = check consumer->getCommittedOffset(topicPartition);
# ```
#
# + partition - The `TopicPartition` in which the committed offset is returned to the consumer
# + duration - Timeout duration (in seconds) for the `getCommittedOffset` operation to execute
# + return - The last committed offset for a given partition for the consumer if there is a committed offset
#            present, `()` if there are no committed offsets, or else a `kafka:Error`
isolated remote function getCommittedOffset(TopicPartition partition, decimal duration = -1) returns PartitionOffset|Error?;
```
* The last offset can be retrieved using `getEndOffsets()`.
```ballerina
# Retrieves the last offsets for a given set of partitions.
# ```ballerina
# kafka:PartitionOffset[] result = check consumer->getEndOffsets([topicPartition1, topicPartition2]);
# ```
#
# + partitions - Set of partitions to get the last offsets
# + duration - Timeout duration (in seconds) for the `getEndOffsets` operation to execute
# + return - End offsets for the given partitions if executes successfully or else a `kafka:Error`
isolated remote function getEndOffsets(TopicPartition[] partitions, decimal duration = -1) returns PartitionOffset[]|Error;
```
* To retrieve the offset of the next record that will be fetched if a record exists in that position, `getPositionOffset()` 
can be used.
```ballerina
# Retrieves the offset of the next record that will be fetched if a record exists in that position.
# ```ballerina
# int result = check consumer->getPositionOffset(topicPartition);
# ```
#
# + partition - The `TopicPartition` in which the position is required
# + duration - Timeout duration (in seconds) for the get position offset operation to execute
# + return - Offset, which will be fetched next (if a record exists in that offset) or else a `kafka:Error` if
#            the operation fails
isolated remote function getPositionOffset(TopicPartition partition, decimal duration = -1) returns int|Error;
```
#### 4.2.4. Handle Partitions
* To assign a consumer to a set of topic partitions, `assign()` can be used.
```ballerina
# Assigns consumer to a set of topic partitions.
#
# + partitions - Topic partitions to be assigned
# + return - `kafka:Error` if an error is encountered or else nil
isolated remote function assign(TopicPartition[] partitions) returns Error?;
```
* To check the assigned topic partitions for a specific consumer, `getAssignment()` can be used.
```ballerina
# Retrieves the currently-assigned partitions of the consumer.
# ```ballerina
# kafka:TopicPartition[] result = check consumer->getAssignment();
# ```
#
# + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:Error`
isolated remote function getAssignment() returns TopicPartition[]|Error;
```
* To pause the message retrieval from a specific set of partitions, `pause()` can be used.
```ballerina
# Pauses retrieving messages from a set of partitions.
# ```ballerina
# kafka:Error? result = consumer->pause([topicPartition1, topicPartition2]);
# ```
#
# + partitions - Set of topic partitions to pause the retrieval of messages
# + return - A `kafka:Error` if an error is encountered or else `()`
isolated remote function pause(TopicPartition[] partitions) returns Error?;
```
* To resume the message retrieval from paused partitions, `resume()` can be used.
```ballerina
# Resumes retrieving messages from a set of partitions, which were paused earlier.
# ```ballerina
# kafka:Error? result = consumer->resume([topicPartition1, topicPartition2]);
# ```
#
# + partitions - Topic partitions to resume the retrieval of messages
# + return - A `kafka:Error` if an error is encountered or else `()`
isolated remote function resume(TopicPartition[] partitions) returns Error?;
```
* To get a list of the partitions that are currently paused from message retrieval, `getPausedPartitions()` can be used.
```ballerina
# Retrieves the partitions, which are currently paused.
# ```ballerina
# kafka:TopicPartition[] result = check consumer->getPausedPartitions();
# ```
#
# + return - The set of partitions paused from message retrieval if executes successfully or else a `kafka:Error`
isolated remote function getPausedPartitions() returns TopicPartition[]|Error;
```
* To get the partitions to which a topic belong, `getTopicPartitions()` can be used.
```ballerina
# Retrieves the set of partitions to which the topic belongs.
# ```ballerina
# kafka:TopicPartition[] result = check consumer->getTopicPartitions("kafka-topic");
# ```
#
# + topic - The topic for which the partition information is needed
# + duration - Timeout duration (in seconds) for the `getTopicPartitions` operation to execute
# + return - Array of partitions for the given topic if executes successfully or else a `kafka:Error`
isolated remote function getTopicPartitions(string topic, decimal duration = -1) returns TopicPartition[]|Error;
```
#### 4.2.5. Seeking
* To seek to a given offset in a topic partition, `seek()` can be used.
```ballerina
# Seeks for a given offset in a topic partition.
# ```ballerina
# kafka:Error? result = consumer->seek(partitionOffset);
# ```
#
# + offset - The `PartitionOffset` to seek
# + return - A `kafka:Error` if an error is encountered or else `()`
isolated remote function seek(PartitionOffset offset) returns Error?;
```
* `seekToBeginning()` can be used to seek to the beginning of the offsets for a given set of topic partitions.
```ballerina
# Seeks to the beginning of the offsets for a given set of topic partitions.
# ```ballerina
# kafka:Error? result = consumer->seekToBeginning([topicPartition1, topicPartition2]);
# ```
#
# + partitions - The set of topic partitions to seek
# + return - A `kafka:Error` if an error is encountered or else `()`
isolated remote function seekToBeginning(TopicPartition[] partitions) returns Error?;
```
* `()` can be used to seek to the end of the offsets for a given set of topic partitions.
```ballerina
# Seeks to the end of the offsets for a given set of topic partitions.
# ```ballerina
# kafka:Error? result = consumer->seekToEnd([topicPartition1, topicPartition2]);
# ```
#
# + partitions - The set of topic partitions to seek
# + return - A `kafka:Error` if an error is encountered or else `()`
isolated remote function seekToEnd(TopicPartition[] partitions) returns Error?;
```
#### 4.2.6. Handle subscriptions
* To get the topics that the consumer is currently subscribed, `getSubscription()` can be used.
```ballerina
# Retrieves the set of topics, which are currently subscribed by the consumer.
# ```ballerina
# string[] result = check consumer->getSubscription();
# ```
#
# + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:Error`
isolated remote function getSubscription() returns string[]|Error;
```
* To get the list of topics currently available (authorized) for the consumer to subscribe, `getAvailableTopics()` can be 
used.
```ballerina
# Retrieves the available list of topics for a particular consumer.
# ```ballerina
# string[] result = check consumer->getAvailableTopics();
# ```
#
# + duration - Timeout duration (in seconds) for the execution of the `getAvailableTopics` operation
# + return - Array of topics currently available (authorized) for the consumer to subscribe or else
#            a `kafka:Error`
isolated remote function getAvailableTopics(decimal duration = -1) returns string[]|Error;
```
* To subscribe to a set of topics, `subscribe()` can be used.
```ballerina
# Subscribes the consumer to the provided set of topics.
# ```ballerina
# kafka:Error? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
# ```
#
# + topics - The array of topics to subscribe
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function subscribe(string[] topics) returns Error?;
```
* To subscribe to a set of topics that match a specific patter, `subscribeWithPattern()` can be used.
```ballerina
# Subscribes the consumer to the topics, which match the provided pattern.
# ```ballerina
# kafka:Error? result = consumer->subscribeWithPattern("kafka.*");
# ```
#
# + regex - The pattern, which should be matched with the topics to be subscribed
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function subscribeWithPattern(string regex) returns Error?;
```
* To unsubscribe from all the topics that the consumer is subscribed to, `unsubscribe()` can be used.
```ballerina
# Unsubscribes from all the topics that the consumer is subscribed to.
# ```ballerina
# kafka:Error? result = consumer->unsubscribe();
# ```
#
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function unsubscribe() returns Error?;
```
### 4.3. Listener
Ballerina Kafka Listener provides the ability to stream messages without manually polling the Kafka server.
#### 4.3.1. Initialization
Connection with the Kafka server can be established insecurely or securely as same as the producer.
##### 4.3.1.1. Insecure Listener
A simple insecure connection with the Kafka server can be easily established by providing the Kafka server URL and
basic configuration.
```ballerina
# Creates a new Kafka `Listener`.
#
# + bootstrapServers - List of remote server endpoints of Kafka brokers
# + config - Configurations related to the consumer endpoint
# + return - A `kafka:Error` if an error is encountered or else '()'
public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error?;
```
##### 4.3.1.2. Secure Listener
A secure client can be established via SSL as same as the Kafka Producer using either a `crypto:Truststore` or a
certificate file. Additionally, a `crypto:Keystore` or a key file can also be provided.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    topics: ["kafka-topic"],
    groupId: "listener-group",
    clientId: "secure-listener",
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SSL
};
```
Consumer client can be authenticated with the Kafka server using SASL by providing `kafka:AuthenticationConfiguration`.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    clientId: "secure-listener",
    groupId: "listener-group",
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};
```
#### 4.3.2. Usage
After initializing the listener, a service must be attached to the listener. There are two ways for this. 
1. Attach the service to the listener directly.
```ballerina
service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        // process results
    }
}
```
2. Attach the service dynamically.
```ballerina
// Create a service object
kafka:Service listenerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        // process results
    }
};
```
The remote function `onConsumerRecord()` is called when the listener receives messages from the Kafka server.

As same as the consumer, if the metadata of `kafka:AnydataConsumerRecord` is not needed, payload can be directly received as well.
```ballerina
kafka:Service listenerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, Person[] payload) returns error? {
        // process results
    }
};
```

The Listener has following functions to manage a service.
* `attach()` - can be used to attach a service to the listener dynamically.
```ballerina
# Attaches a service to the listener.
#
# + s - The service to be attached
# + name - Name of the service
# + return - An `kafka:Error` if an error is encountered while attaching the service or else nil
public isolated function attach(Service s, string[]|string? name = ()) returns error?;
```
* `detach()` - can be used to detach a service from the listener.
```ballerina
# Detaches a consumer service from the listener.
#
# + s - The service to be detached
# + return - A `kafka:Error` if an error is encountered while detaching a service or else `()`
public isolated function detach(Service s) returns error?;
```
* `start()` - needs to be called to start the listener.
```ballerina
# Starts the registered services.
#
# + return - An `kafka:Error` if an error is encountered while starting the server or else nil
public isolated function 'start() returns error?;
```
* `gracefulStop()` - can be used to gracefully stop the listener from consuming messages.
```ballerina
# Stops the Kafka listener gracefully.
#
# + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
public isolated function gracefulStop() returns error?;
```
* `immediateStop()` - can be used to immediately stop the listener from consuming messages.
```ballerina
# Stops the kafka listener immediately.
#
# + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
public isolated function immediateStop() returns error?;
```
If the `autoCommit` configuration of the listener is `false`, the consumed offsets will not be committed. In order to manually 
control this, the Caller API can be used.
#### 4.3.3. Caller
* To commit the consumed offsets, `commit()` can be used.
```ballerina
# Commits the currently consumed offsets of the service.
# ```ballerina
# kafka:Error? result = caller->commit();
# ```
#
# + return - A `kafka:Error` if an error is encountered or else '()'
isolated remote function 'commit() returns Error?;
```
* To commit a specific offset, `commitOffset()` can be used.
```ballerina
# Commits the given offsets and partitions for the given topics of the service.
#
# + offsets - Offsets to be commited
# + duration - Timeout duration (in seconds) for the commit operation execution
# + return - `kafka:Error` if an error is encountered or else nil
isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error?;
```
## 5. Samples
### 5.1. Produce Messages
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
    check kafkaProducer->send({ topic: "kafka-topic", value: message });
    check kafkaProducer->'flush();
    check kafkaProducer->close();
}
```
### 5.2. Consume Messages
#### 5.2.1. Using Consumer Client
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

public type Order record {|
    int orderId;
    string productName;
    boolean paymentValid;
|};

public type OrderConsumerRecord record {|
    *kafka:AnydataConumerRecord;
    Order value;
|};

kafka:Consumer consumer = check new (kafka:DEFAULT_URL, consumerConfiguration);

public function main() returns error? {
    check consumer->subscribe(["kafka-topic"]);
    OrderConsumerRecord[] records = check consumer->poll(1);
    
    check from OrderConsumerRecord orderRecord in records
        where orderRecord.value.paymentValid
        do {
            io:println("Received Order: " + orderRecord.value.productName);
        };
}
```
#### 5.2.2. Using Listener
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

public type Order record {|
    int orderId;
    string productName;
    boolean paymentValid;
|};

public type OrderConsumerRecord record {|
    *kafka:AnydataConumerRecord;
    Order value;
|};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

public function main() returns error? {
    check kafkaListener.attach(listenerService);
    check kafkaListener.'start();
    runtime:registerListener(kafkaListener);
}

kafka:Service listenerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, OrderConsumerRecord[] records) returns error? {
        check from OrderConsumerRecord orderRecord in records
           where orderRecord.value.paymentValid
           do {
               log:printInfo("Received Order: " + orderRecord.value.productName);
           };
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer ", 'error = commitResult);
        }
    }
};
```
