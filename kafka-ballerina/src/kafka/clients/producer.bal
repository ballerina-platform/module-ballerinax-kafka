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

# Represents a Kafka producer endpoint.
#
# + connectorId - Unique ID for a particular connector to use in trasactions
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
    public remote function send(anydata value, string topic, anydata? key = (), int? partition = (),
        int? timestamp = ()) returns ProducerError? {
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
