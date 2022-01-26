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

import ballerina/uuid;
import ballerina/jballerina.java;

# Represents a Kafka producer endpoint.
#
# + connectorId - Unique ID for a particular connector to use in trasactions
# + producerConfig - Stores configurations related to a Kafka connection
public client isolated class Producer {

    final ProducerConfiguration? & readonly producerConfig;
    private final string keySerializerType;
    private final string valueSerializerType;
    private final string|string[] & readonly bootstrapServers;

    private string connectorId = uuid:createType4AsString();

    # Creates a new `kafka:Producer`.
    #
    # + bootstrapServers - List of remote server endpoints of Kafka brokers
    # + config - Configurations related to initializing a `kafka:Producer`
    # + return - A `kafka:Error` if closing the producer failed or else '()'
    public isolated function init(string|string[] bootstrapServers, *ProducerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers.cloneReadOnly();
        self.producerConfig = config.cloneReadOnly();
        self.keySerializerType = SER_BYTE_ARRAY;
        self.valueSerializerType = SER_BYTE_ARRAY;

        check self.producerInit();
    }

    private isolated function producerInit() returns Error? =
    @java:Method {
        name: "init",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.ProducerActions"
    } external;

    # Closes the producer connection to the external Kafka broker.
    # ```ballerina
    # kafka:Error? result = producer->close();
    # ```
    #
    # + return - A `kafka:Error` if closing the producer failed or else '()'
    isolated remote function close() returns Error? =
    @java:Method {
        'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.ProducerActions"
    } external;

    # Flushes the batch of records already sent to the broker by the producer.
    # ```ballerina
    # kafka:Error? result = producer->'flush();
    # ```
    #
    # + return - A `kafka:Error` if records couldn't be flushed or else '()'
    isolated remote function 'flush() returns Error? =
    @java:Method {
        name: "flushRecords",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.ProducerActions"
    } external;

    # Retrieves the topic partition information for the provided topic.
    # ```ballerina
    # kafka:TopicPartition[] result = check producer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - The specific topic, of which the topic partition information is required
    # + return - A `kafka:TopicPartition` array for the given topic or else a `kafka:Error` if the operation fails
    isolated remote function getTopicPartitions(string topic) returns TopicPartition[]|Error =
    @java:Method {
        'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.ProducerActions"
    } external;

    # Produces records to the Kafka server.
    # ```ballerina
    # kafka:Error? result = producer->send({value: "Hello World".toBytes(), topic: "kafka-topic"});
    # ```
    #
    # + producerRecord - Record to be produced
    # + return -  A `kafka:Error` if send action fails to send data or else '()'
    isolated remote function send(ProducerRecord producerRecord) returns Error? {
        // Only producing byte[] values is handled at the moment
        return sendByteArrayValues(self, producerRecord.value, producerRecord.topic, producerRecord?.key,
        producerRecord?.partition, producerRecord?.timestamp, self.keySerializerType);
    }
}
