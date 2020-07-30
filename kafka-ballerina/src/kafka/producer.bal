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

import ballerina/system;

# Represents the Kafka Producer configuration.
#
# + bootstrapServers - List of remote server endpoints of Kafka brokers
# + acks - Number of acknowledgments
# + compressionType - Compression type to be used for messages
# + clientId - Identifier to be used for server side logging
# + metricsRecordingLevel - Metrics recording level
# + metricReporterClasses - Metrics reporter classes
# + partitionerClass - Partitioner class to be used to select the partition to which the message is sent
# + interceptorClasses - Interceptor classes to be used before sending records
# + transactionalId - Transactional ID to be used in transactional delivery
# + keySerializerType - Serializer used for the Kafka record key. This can be either `kafka:SerializerType` or a
#                       user-defined serializer
# + valueSerializerType - Serializer used for the Kafka record value. This can be either `kafka:SerializerType` or a
#                         user-defined serializer
# + keySerializer - Custom serializer object to serialize Kafka keys. This should implement the `kafka:Serializer`
#                   object
# + valueSerializer - Custom serializer object to serialize Kafka values. This should implement the
#                     `kafka:Serializer` object
# + schemaRegistryUrl - Avro schema registry URL. Use this field to specify the schema registry URL if the Avro
#                       serializer is used
# + additionalProperties - Additional properties for the property fields not provided by Ballerina Kafka module. Use
#                          this with caution since this can override any of the fields. It is not recomendded to use
#                          this field except in an extreme situation
# + bufferMemory - Total bytes of memory the producer can use to buffer records
# + retryCount - Number of retries to resend a record
# + batchSize - Number of records to be batched for a single request. Use 0 for no batching
# + lingerInMillis - Delay to allow other records to be batched before sending them to the Kafka server
# + sendBuffer - Size of the TCP send buffer (SO_SNDBUF)
# + receiveBuffer - Size of the TCP receive buffer (SO_RCVBUF)
# + maxRequestSize - The maximum size of a request in bytes
# + reconnectBackoffTimeInMillis - Time to wait before attempting to reconnect
# + reconnectBackoffMaxTimeInMillis - Maximum amount of time in milliseconds to wait when reconnecting
# + retryBackoffTimeInMillis - Time to wait before attempting to retry a failed request
# + maxBlockInMillis - Maximum block time during which the sending is blocked when the buffer is full
# + requestTimeoutInMillis - Wait time for the response of a request
# + metadataMaxAgeInMillis - Maximum time to force a refresh of metadata
# + metricsSampleWindowInMillis - Time window for a metrics sample to compute over
# + metricsNumSamples - Number of samples maintained to compute the metrics
# + maxInFlightRequestsPerConnection - Maximum number of unacknowledged requests on a single connection
# + connectionsMaxIdleTimeInMillis - Close the idle connections after this number of milliseconds
# + transactionTimeoutInMillis - Timeout for transaction status update from the producer
# + enableIdempotence - Exactly one copy of each message is written to the stream when enabled
# + secureSocket - Configurations related to SSL/TLS encryption
# + authenticationConfiguration - Authentication-related configurations for the Kafka producer
public type ProducerConfiguration record {|
    string bootstrapServers;
    ProducerAcks acks = ACKS_SINGLE;
    CompressionType compressionType = COMPRESSION_NONE;
    string clientId?;
    string metricsRecordingLevel?;
    string metricReporterClasses?;
    string partitionerClass?;
    string interceptorClasses?;
    string transactionalId?;

    SerializerType valueSerializerType = SER_BYTE_ARRAY;
    SerializerType keySerializerType = SER_BYTE_ARRAY;
    Serializer valueSerializer?;
    Serializer keySerializer?;
    string schemaRegistryUrl?;

    map<string> additionalProperties?;

    int bufferMemory?;
    int retryCount?;
    int batchSize?;
    int lingerInMillis?;
    int sendBuffer?;
    int receiveBuffer?;
    int maxRequestSize?;
    int reconnectBackoffTimeInMillis?;
    int reconnectBackoffMaxTimeInMillis?;
    int retryBackoffTimeInMillis?;
    int maxBlockInMillis?;
    int requestTimeoutInMillis?;
    int metadataMaxAgeInMillis?;
    int metricsSampleWindowInMillis?;
    int metricsNumSamples?;
    int maxInFlightRequestsPerConnection?;
    int connectionsMaxIdleTimeInMillis?;
    int transactionTimeoutInMillis?;

    boolean enableIdempotence = false;

    SecureSocket secureSocket?;
    AuthenticationConfiguration authenticationConfiguration?;
|};

# Defines a record to send data using Avro serialization.
#
# + schemaString - The string, which defines the Avro schema
# + dataRecord - Records, which should be serialized using Avro
public type AvroRecord record {|
    string schemaString;
    anydata dataRecord;
|};

# Kafka producer acknowledgement types.
public type ProducerAcks ACKS_ALL|ACKS_NONE|ACKS_SINGLE;

# Kafka in-built serializer types.
public type SerializerType SER_BYTE_ARRAY|SER_STRING|SER_INT|SER_FLOAT|SER_AVRO|SER_CUSTOM;

# Kafka compression types to compress the messages.
public type CompressionType COMPRESSION_NONE|COMPRESSION_GZIP|COMPRESSION_SNAPPY|COMPRESSION_LZ4|COMPRESSION_ZSTD;

# Represents a Kafka producer endpoint.
#
# + connectorId - Unique ID for a particular connector
# + producerConfig - Used to store configurations related to a Kafka connection
public type Producer client object {

    public ProducerConfiguration? producerConfig = ();
    private string keySerializerType;
    private string valueSerializerType;
    private Serializer? keySerializer = ();
    private Serializer? valueSerializer = ();

    # Creates a new Kafka `Producer`.
    #
    # + config - Configurations related to initializing a Kafka `Producer`
    public function init(ProducerConfiguration config) {
        self.producerConfig = config;
        self.keySerializerType = config.keySerializerType;
        self.valueSerializerType = config.valueSerializerType;

        if (self.keySerializerType == SER_CUSTOM) {
            var keySerializerObject = config?.keySerializer;
            if (keySerializerObject is ()) {
                panic createProducerError("Invalid keySerializer config: Please Provide a " +
                            "valid custom serializer for the keySerializer");
            } else {
                self.keySerializer = keySerializerObject;
            }
        }
        if (self.keySerializerType == SER_AVRO) {
            var schemaRegistryUrl = config?.schemaRegistryUrl;
            if (schemaRegistryUrl is ()) {
                panic createProducerError("Missing schema registry URL for the Avro serializer. Please " +
                            "provide 'schemaRegistryUrl' configuration in 'kafka:ProducerConfiguration'.");
            }
        }
        if (self.valueSerializerType == SER_CUSTOM) {
            var valueSerializerObject = config?.valueSerializer;
            if (valueSerializerObject is ()) {
                panic createProducerError("Invalid valueSerializer config: Please Provide a " +
                            "valid custom serializer for the valueSerializer");
            } else {
                self.valueSerializer = valueSerializerObject;
            }
        }
        if (self.valueSerializerType == SER_AVRO) {
            var schemaRegistryUrl = config?.schemaRegistryUrl;
            if (schemaRegistryUrl is ()) {
                panic createProducerError("Missing schema registry URL for the Avro serializer. Please " +
                            "provide 'schemaRegistryUrl' configuration in 'kafka:ProducerConfiguration'.");
            }
        }
        checkpanic producerInit(self);
    }

    public string connectorId = system:uuid();

    # Closes the producer connection to the external Kafka broker.
    # ```ballerina
    # kafka:ProducerError? result = producer->close();
    # ```
    #
    # + return - A `kafka:ProducerError` if closing the producer failed or else '()'
    public remote function close() returns ProducerError? {
        return producerClose(self);
    }

    # Commits the offsets consumed by the provided consumer.
    #
    # + consumer - Consumer, which needs offsets to be committed
    # + return - A`kafka:ProducerError` if committing the consumer failed or else ()
    public remote function commitConsumer(Consumer consumer) returns ProducerError? {
        return producerCommitConsumer(self, consumer);
    }

    # Commits the consumer offsets in a given transaction.
    #
    # + offsets - Consumer offsets to commit for a given transaction
    # + groupID - Consumer group ID
    # + return - A `kafka:ProducerError` if committing consumer offsets failed or else ()
    public remote function commitConsumerOffsets(PartitionOffset[] offsets, string groupID) returns ProducerError? {
        return producerCommitConsumerOffsets(self, offsets, groupID);
    }

    # Flushes the batch of records already sent to the broker by the producer.
    # ```ballerina
    # kafka:ProducerError? result = producer->flushRecords();
    # ```
    #
    # + return - A `kafka:ProducerError` if records couldn't be flushed or else '()'
    public remote function flushRecords() returns ProducerError? {
        return producerFlushRecords(self);
    }

    # Retrieves the topic partition information for the provided topic.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ProducerError result = producer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - Topic of which the partition information is given
    # + return - A `kafka:TopicPartition` array for the given topic or else a `kafka:ProducerError` if the operation fails
    public remote function getTopicPartitions(string topic) returns TopicPartition[]|ProducerError {
        return producerGetTopicPartitions(self, topic);
    }

    # Produces records to the Kafka server.
    # ```ballerina
    # kafka:ProducerError? result = producer->send("Hello World, Ballerina", "kafka-topic");
    # ```
    #
    # + value - Record contents
    # + topic - Topic to which the record will be appended
    # + key - Key, which will be included in the record
    # + partition - Partition to which the record should be sent
    # + timestamp - Timestamp of the record in milliseconds since epoch
    # + return -  A `kafka:ProducerError` if send action fails to send data or else '()'
    public remote function send(anydata value, string topic, public anydata? key = (), public int? partition = (),
        public int? timestamp = ()) returns ProducerError? {
        // Handle string values
        if (self.valueSerializerType == SER_STRING) {
            if (value is string) {
                return sendStringValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
            }
            panic getValueTypeMismatchError(STRING);
        }
        // Handle int values
        if (self.valueSerializerType == SER_INT) {
            if (value is int) {
                return sendIntValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
            }
            panic getValueTypeMismatchError(INT);
        }
        // Handle float values
        if (self.valueSerializerType == SER_FLOAT) {
            if (value is float) {
                return sendFloatValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
            }
            panic getValueTypeMismatchError(FLOAT);
        }
        // Handle byte[] values
        if (self.valueSerializerType == SER_BYTE_ARRAY) {
            if (value is byte[]) {
                return sendByteArrayValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
            }
            panic getValueTypeMismatchError(BYTE_ARRAY);
        }
        // Handle Avro serializer.
        if (self.valueSerializerType == SER_AVRO) {
            if (value is AvroRecord) {
                return sendAvroValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
            }
            panic getValueTypeMismatchError(AVRO_RECORD);
        }
        // Handle custom values
        if (self.valueSerializerType == SER_CUSTOM) {
            return sendCustomValues(self, value, topic, key, partition, timestamp, self.keySerializerType);
        }
        panic createProducerError("Invalid value serializer configuration");
    }
};
