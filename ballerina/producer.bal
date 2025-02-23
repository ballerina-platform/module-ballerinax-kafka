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
    private final anydata schemaRegistryConfig;
    private final string? schema;

    private string connectorId = uuid:createType4AsString();

    # Creates a new `kafka:Producer`.
    #
    # + bootstrapServers - List of remote server endpoints of Kafka brokers
    # + config - Configurations related to initializing a `kafka:Producer`
    # + return - A `kafka:Error` if closing the producer failed or else '()'
    public isolated function init(string|string[] bootstrapServers, *ProducerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers.cloneReadOnly();
        self.producerConfig = config.cloneReadOnly();
        self.keySerializerType = config.keySerializerType;
        self.valueSerializerType = config.valueSerializerType;
        self.schemaRegistryConfig = config.schemaRegistryConfig.cloneReadOnly();
        self.schema = config?.avroSchema;
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
    # + return - A `kafka:Error` if send action fails to send data or else '()'
    isolated remote function send(AnydataProducerRecord producerRecord) returns Error? {
        // Only producing byte[] values is handled at the moment
        anydata anydataValue = producerRecord.value;
        byte[] value = anydataValue.toJsonString().toBytes();
        byte[]? key = ();
        anydata anydataKey = producerRecord?.key;

        boolean isKeyAvro = self.keySerializerType == SER_AVRO;
        boolean isValueAvro = self.valueSerializerType == SER_AVRO;
        anydata & readonly schemaRegistryConfig;
        string? schema;
        lock {
            schemaRegistryConfig = self.schemaRegistryConfig.cloneReadOnly();
            schema = self.schema.cloneReadOnly();
        }
        if isKeyAvro && anydataKey != () {
            do {
                if schema is () {
                    return error Error("The field `schema` cannot be empty for Avro serialization");
                }
                Serializer serializer = check new AvroSerializer(schemaRegistryConfig, schema);
                key = check serializer.serialize(anydataKey, schema);
            } on fail error err {
                return error Error(err.message());
            }
        }
        if isValueAvro {
            do {
                if schema is () {
                    return error Error("The field `schema` cannot be empty for Avro serialization");
                }
                Serializer serializer = check new AvroSerializer(schemaRegistryConfig, schema);
                value = check serializer.serialize(anydataValue, schema);
            } on fail error err {
                return error Error(err.message());
            }
        }
        if !isKeyAvro && !isValueAvro {
            if anydataValue is byte[] {
                value = anydataValue;
            } else if anydataValue is xml {
                value = anydataValue.toString().toBytes();
            } else if anydataValue is string {
                value = anydataValue.toBytes();
            }
            if anydataKey is byte[] {
                key = anydataKey;
            } else if anydataKey is xml {
                key = anydataKey.toString().toBytes();
            } else if anydataKey is string {
                key = anydataKey.toBytes();
            } else if anydataKey !is () {
                key = anydataKey.toJsonString().toBytes();
            }
        }
        return sendByteArrayValues(self, value, producerRecord.topic, self.getHeaderValueAsByteArrayList(producerRecord?.headers), key,
        producerRecord?.partition, producerRecord?.timestamp, self.keySerializerType);
    }

    private isolated function getHeaderValueAsByteArrayList(map<byte[]|byte[][]|string|string[]>? headers) returns [string, byte[]][] {
        [string, byte[]][] bytesHeaderList = [];
        if headers is map<byte[]|byte[][]|string|string[]> {
            foreach string key in headers.keys() {
                byte[]|byte[][]|string|string[] values = headers.get(key);
                if values is byte[] {
                    bytesHeaderList.push([key, values]);
                } else if values is byte[][] {
                    foreach byte[] headerValue in values {
                        bytesHeaderList.push([key, headerValue]);
                    }
                } else if values is string {
                    bytesHeaderList.push([key, values.toBytes()]);
                } else if values is string[] {
                    foreach string headerValue in values {
                        bytesHeaderList.push([key, headerValue.toBytes()]);
                    }
                }
            }
        }
        return bytesHeaderList;
    }
}
