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

# Represents a Kafka producer endpoint.
#
# + connectorId - Unique ID for a particular connector to use in trasactions
# + producerConfig - Used to store configurations related to a Kafka connection
public client class Producer {

    public ProducerConfiguration? producerConfig = ();
    private string keySerializerType;
    private string valueSerializerType;

    # Creates a new Kafka `Producer`.
    #
    # + config - Configurations related to initializing a Kafka `Producer`
    public isolated function init(ProducerConfiguration config) {
        self.producerConfig = config;
        self.keySerializerType = config.keySerializerType;
        self.valueSerializerType = config.valueSerializerType;

        checkpanic producerInit(self);
    }

    string connectorId = uuid:createType4AsString();

    # Closes the producer connection to the external Kafka broker.
    # ```ballerina
    # kafka:ProducerError? result = producer->close();
    # ```
    #
    # + return - A `kafka:ProducerError` if closing the producer failed or else '()'
    isolated remote function close() returns ProducerError? {
        return producerClose(self);
    }

    # Flushes the batch of records already sent to the broker by the producer.
    # ```ballerina
    # kafka:ProducerError? result = producer->flushRecords();
    # ```
    #
    # + return - A `kafka:ProducerError` if records couldn't be flushed or else '()'
    isolated remote function flushRecords() returns ProducerError? {
        return producerFlushRecords(self);
    }

    # Retrieves the topic partition information for the provided topic.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ProducerError result = producer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - Topic of which the partition information is given
    # + return - A `kafka:TopicPartition` array for the given topic or else a `kafka:ProducerError` if the operation fails
    isolated remote function getTopicPartitions(string topic) returns TopicPartition[]|ProducerError {
        return producerGetTopicPartitions(self, topic);
    }

    # Produces records to the Kafka server.
    # ```ballerina
    # kafka:ProducerError? result = producer->send("Hello World, Ballerina", "kafka-topic");
    # ```
    #
    # + producerRecord - Record to be produced
    # + return -  A `kafka:ProducerError` if send action fails to send data or else '()'
    isolated remote function sendProducerRecord(ProducerRecord producerRecord) returns ProducerError? {
        // Only producing byte[] values is handled at the moment
        if (self.valueSerializerType == SER_BYTE_ARRAY) {
            return sendByteArrayValues(self, producerRecord.value, producerRecord.topic, producerRecord?.key,
            producerRecord?.partition, producerRecord?.timestamp, self.keySerializerType);
        }
        panic createProducerError("Invalid value serializer configuration");
    }
}
