// Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/crypto;


// Security-related records
# Configurations for secure communication with the Kafka server.
#
# + cert - Configurations associated with crypto:TrustStore or single certificate file that the client trusts
# + key - Configurations associated with crypto:KeyStore or combination of certificate and private key of the client
# + protocol - SSL/TLS protocol related options
# + ciphers - List of ciphers to be used. By default, all the available cipher suites are supported
# + provider - Name of the security provider used for SSL connections. The default value is the default security provider
#              of the JVM
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

# Represents a combination of certificate, private key, and private key password if encrypted.
#
# + certFile - A file containing the certificate
# + keyFile - A file containing the private key in PKCS8 format
# + keyPassword - Password of the private key if it is encrypted
public type CertKey record {|
    string certFile;
    string keyFile;
    string keyPassword?;
|};

# Represents protocol options.
public enum Protocol {
   SSL,
   TLS,
   DTLS
}

# Configurations related to Kafka authentication mechanisms.
#
# + mechanism - Type of the authentication mechanism. Currently `SASL_PLAIN`, `SASL_SCRAM_256` & `SASL_SCRAM_512`
#               is supported
# + username - The username to authenticate the Kafka producer/consumer
# + password - The password to authenticate the Kafka producer/consumer
public type AuthenticationConfiguration record {|
    AuthenticationMechanism mechanism = AUTH_SASL_PLAIN;
    string username;
    string password;
|};

// Consumer-related records
# Configurations related to consumer endpoint.
#
# + groupId - Unique string that identifies the consumer
# + topics - Topics to be subscribed by the consumer
# + offsetReset - Offset reset strategy if no initial offset
# + partitionAssignmentStrategy - Strategy class for handling the partition assignment among consumers
# + metricsRecordingLevel - Metrics recording level
# + metricsReporterClasses - Metrics reporter classes
# + clientId - Identifier to be used for server side logging
# + interceptorClasses - Interceptor classes to be used before sending the records
# + isolationLevel - Transactional message reading method
# + schemaRegistryConfig - Configurations to initialize a schema registry
# + keyDeserializerType - Key deserialization type
# + valueDeserializerType - Value deserialization type
# + schemaRegistryUrl - Avro schema registry URL. Use this field to specify the schema registry URL, if the Avro serializer
#                       is used
# + additionalProperties - Additional properties for the property fields not provided by the Ballerina `kafka` module. Use
#                          this with caution since this can override any of the fields. It is not recomendded to use
#                          this field except in an extreme situation
# + sessionTimeout - Timeout (in seconds) used to detect consumer failures when the heartbeat threshold is reached
# + heartBeatInterval - Expected time (in seconds) between the heartbeats
# + metadataMaxAge - Maximum time (in seconds) to force a refresh of metadata
# + autoCommitInterval - Auto committing interval (in seconds) for commit offset when auto-committing is enabled
# + maxPartitionFetchBytes - The maximum amount of data the server returns per partition
# + sendBuffer - Size of the TCP send buffer (SO_SNDBUF)
# + receiveBuffer - Size of the TCP receive buffer (SO_RCVBUF)
# + fetchMinBytes - Minimum amount of data the server should return for a fetch request
# + fetchMaxBytes - Maximum amount of data the server should return for a fetch request
# + fetchMaxWaitTime - Maximum amount of time (in seconds) the server will block before answering the fetch request
# + reconnectBackoffTimeMax - Maximum amount of time in seconds to wait when reconnecting
# + retryBackoff - Time (in seconds) to wait before attempting to retry a failed request
# + metricsSampleWindow - Window of time (in seconds) a metrics sample is computed over
# + metricsNumSamples - Number of samples maintained to compute metrics
# + requestTimeout - Wait time (in seconds) for response of a request
# + connectionMaxIdleTime - Close idle connections after the number of seconds
# + maxPollRecords - Maximum number of records returned in a single call to poll
# + maxPollInterval - Maximum delay between invocations of poll
# + reconnectBackoffTime - Time (in seconds) to wait before attempting to reconnect
# + pollingTimeout - Timeout interval for polling in seconds
# + pollingInterval - Polling interval for the consumer in seconds
# + concurrentConsumers - Number of concurrent consumers
# + defaultApiTimeout - Default API timeout value (in seconds) for APIs with duration
# + autoCommit - Enables auto committing offsets
# + checkCRCS - Checks the CRC32 of the records consumed. This ensures that no on-the-wire or on-disk corruption occurred
#               to the messages. This may add some overhead and might need to be set to `false` if extreme
#               performance is required
# + excludeInternalTopics - Whether records from internal topics should be exposed to the consumer
# + decoupleProcessing - Decouples processing
# + validation - Configuration related to constraint validation check
# + autoSeekOnValidationFailure - Automatically seeks past the erroneous records in the event of an data-binding or
#                                 validating constraints failure
# + secureSocket - Configurations related to SSL/TLS encryption
# + auth - Authentication-related configurations for the `kafka:Consumer`
# + securityProtocol - Type of the security protocol to use in the broker connection
public type ConsumerConfiguration record {|
    string groupId?;
    string|string[] topics?;
    OffsetResetMethod offsetReset?;
    string partitionAssignmentStrategy?;
    string metricsRecordingLevel?;
    string metricsReporterClasses?;
    string clientId?;
    string interceptorClasses?;
    IsolationLevel isolationLevel?;

    string schemaRegistryUrl?;
    readonly map<anydata> schemaRegistryConfig?;
    DeserializerType keyDeserializerType = DES_BYTE_ARRAY;
    DeserializerType valueDeserializerType = DES_BYTE_ARRAY;

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
    boolean validation = true;
    boolean autoSeekOnValidationFailure = true;

    SecureSocket secureSocket?;
    AuthenticationConfiguration auth?;
    SecurityProtocol securityProtocol = PROTOCOL_PLAINTEXT;
|};

// Common record types
# Represents the topic partition position in which the consumed record is stored.
#
# + partition - The `kafka:TopicPartition` to which the record is related
# + offset - Offset in which the record is stored in the partition
public type PartitionOffset record {|
    TopicPartition partition;
    int offset;
|};

# Represents a topic partition.
#
# + topic - Topic to which the partition is related
# + partition - Index of the specific partition
public type TopicPartition record {|
    string topic;
    int partition;
|};

# Type related to anydata consumer record.
#
# + key - Key that is included in the record
# + value - Anydata record content
# + timestamp - Timestamp of the record, in milliseconds since epoch
# + offset - Topic partition position in which the consumed record is stored
# + headers - Map of headers included with the record
public type AnydataConsumerRecord record {|
    anydata key?;
    anydata value;
    int timestamp;
    PartitionOffset offset;
    map<byte[]|byte[][]|string|string[]> headers;
|};

# Subtype related to `kafka:AnydataConsumerRecord` record.
#
# + value - Record content in bytes
# + headers - Headers as a byte[] or byte[][]
public type BytesConsumerRecord record {|
    *AnydataConsumerRecord;
    byte[] value;
    map<byte[]|byte[][]> headers;
|};

# Details related to the anydata producer record.
#
# + topic - Topic to which the record will be appended
# + key - Key that is included in the record
# + value - Anydata record content
# + timestamp - Timestamp of the record, in milliseconds since epoch
# + partition - Partition to which the record should be sent
# + headers - Map of headers to be included with the record
public type AnydataProducerRecord record {|
    string topic;
    anydata key?;
    anydata value;
    int timestamp?;
    int partition?;
    map<byte[]|byte[][]|string|string[]> headers?;
|};

# Subtype related to `kafka:AnydataProducerRecord` record.
#
# + value - Record content in bytes
# + headers - Headers as a byte[] or byte[][]
public type BytesProducerRecord record {|
    *AnydataProducerRecord;
    byte[] value;
    map<byte[]|byte[][]> headers?;
|};

// Producer-related records
# Represents the `kafka:Producer` configuration.
#
# + acks - Number of acknowledgments
# + compressionType - Compression type to be used for messages
# + clientId - Identifier to be used for server side logging
# + metricsRecordingLevel - Metrics recording level
# + metricReporterClasses - Metrics reporter classes
# + partitionerClass - Partitioner class to be used to select the partition to which the message is sent
# + interceptorClasses - Interceptor classes to be used before sending the records
# + transactionalId - Transactional ID to be used in transactional delivery
# + schemaRegistryUrl - Avro schema registry URL. Use this field to specify the schema registry URL if the Avro
# serializer is used
# + avroSchema - The schema used for key/value serialization (Deprecated)
# + keySchema - The schema used to serializate the key
# + valueSchema - The schema used to serialize the value
# + schemaRegistryConfig - Configurations to initialize a schema registry
# + keySerializerType - Key serialization type
# + valueSerializerType - Value serialization type
# + additionalProperties - Additional properties for the property fields not provided by the Ballerina `kafka` module. Use
# this with caution since this can override any of the fields. It is not recomendded to use
# this field except in an extreme situation
# + bufferMemory - Total bytes of memory the producer can use to buffer records
# + retryCount - Number of retries to resend a record
# + batchSize - Maximum number of bytes to be batched together when sending the records. Records exceeding this limit will
# not be batched. Setting this to 0 will disable batching
# + linger - Delay (in seconds) to allow other records to be batched before sending them to the Kafka server
# + sendBuffer - Size of the TCP send buffer (SO_SNDBUF)
# + receiveBuffer - Size of the TCP receive buffer (SO_RCVBUF)
# + maxRequestSize - The maximum size of a request in bytes
# + reconnectBackoffTime - Time (in seconds) to wait before attempting to reconnect
# + reconnectBackoffMaxTime - Maximum amount of time in seconds to wait when reconnecting
# + retryBackoffTime - Time (in seconds) to wait before attempting to retry a failed request
# + maxBlock - Maximum block time (in seconds) during which the sending is blocked when the buffer is full
# + requestTimeout - Wait time (in seconds) for the response of a request
# + metadataMaxAge - Maximum time (in seconds) to force a refresh of metadata
# + metricsSampleWindow - Time (in seconds) window for a metrics sample to compute over
# + metricsNumSamples - Number of samples maintained to compute the metrics
# + maxInFlightRequestsPerConnection - Maximum number of unacknowledged requests on a single connection
# + connectionsMaxIdleTime - Close the idle connections after this number of seconds
# + transactionTimeout - Timeout (in seconds) for transaction status update from the producer
# + enableIdempotence - Exactly one copy of each message is written to the stream when enabled
# + secureSocket - Configurations related to SSL/TLS encryption
# + auth - Authentication-related configurations for the `kafka:Producer`
# + securityProtocol - Type of the security protocol to use in the broker connection
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
    string avroSchema?;
    string keySchema?;
    string valueSchema?;
    map<anydata> schemaRegistryConfig?;
    SerializerType keySerializerType = SER_BYTE_ARRAY;
    SerializerType valueSerializerType = SER_BYTE_ARRAY;

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

# Defines the Payload remote function parameter.
public type KafkaPayload record {||};

# The annotation which is used to define the payload parameter in the `onConsumerRecord` service method.
public annotation KafkaPayload Payload on parameter;
