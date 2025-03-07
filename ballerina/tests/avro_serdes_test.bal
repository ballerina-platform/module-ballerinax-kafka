// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
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

import ballerina/test;

const string KEY_SCHEMA = "{\"type\": \"string\", \"name\": \"stringValue\", \"namespace\": \"data\"}";
const string VALUE_SCHEMA = "{\"type\": \"string\", \"name\": \"stringValue\", \"namespace\": \"data\"}";

final string key = "key-message";
final string value = "value-message";

@test:Config {
    enable: true,
    groups: ["serdes"]
}
function keyValueSerializationTest() returns error? {
    string topic = "schema-registry-key-value-test-topic";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        keySerializerType: SER_AVRO,
        valueSerializerType: SER_AVRO,
        keySchema: KEY_SCHEMA,
        valueSchema: VALUE_SCHEMA,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        clientId: "test-producer-1",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };

    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    check producer->send({topic, 'key, value});
    ConsumerConfiguration consumerConfiguration = {
        keyDeserializerType: DES_AVRO,
        valueDeserializerType: DES_AVRO,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-key-value-test-group",
        clientId: "test-key-value-serdes"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    AnydataConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    test:assertEquals(consumerRecords[0]?.key, key);
    test:assertEquals(consumerRecords[0].value, value);
}

@test:Config {
    enable: true,
    groups: ["serdes"]
}
function keySerializationTest() returns error? {
    string topic = "schema-registry-key-test-topic";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        keySerializerType: SER_AVRO,
        keySchema: KEY_SCHEMA,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        clientId: "test-producer-1",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };

    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    check producer->send({topic, 'key, value});
    ConsumerConfiguration consumerConfiguration = {
        keyDeserializerType: DES_AVRO,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-key-serdes-test-group",
        clientId: "test-key-serdes"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    AnydataConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    test:assertEquals(consumerRecords[0]?.key, key);
}

@test:Config {
    enable: true,
    groups: ["serdes"]
}
function valueSerializationTest() returns error? {
    string topic = "schema-registry-value-test-topic";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        valueSerializerType: SER_AVRO,
        valueSchema: VALUE_SCHEMA,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        clientId: "test-producer-1",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };

    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    check producer->send({topic, value});
    ConsumerConfiguration consumerConfiguration = {
        valueDeserializerType: DES_AVRO,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-value-serdes-test-group",
        clientId: "test-value-serdes"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    AnydataConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    test:assertEquals(consumerRecords[0].value, value);
}

@test:Config {
    enable: true,
    groups: ["serdes", "error"]
}
function valueSerializationErrorTest() returns error? {
    string topic = "value-error-test-topic";
    string value = "message";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        valueSerializerType: SER_AVRO,
        valueSchema: string `{"namespace": "example.avro.test", "type": "record", "name": "testStudent", "fields": [{"name": "name", "type": "string"}, {"name": "favorite_color", "type": "string"}]}`,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        clientId: "test-producer-1",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };

    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    error? result = producer->send({topic, value});
    test:assertTrue(result is error);
}

@test:Config {
    enable: true,
    groups: ["serdes", "error"]
}
function keySerializationErrorTest() returns error? {
    string topic = "value-error-test-topic";
    string value = "message";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        keySerializerType: SER_AVRO,
        keySchema: string `{"namespace": "example.avro.test", "type": "record", "name": "testStudent", 
            "fields": [{"name": "name", "type": "string"}, {"name": "favorite_color", "type": "string"}]}`,
        schemaRegistryConfig: {
            "baseUrl": "http://0.0.0.0:8081"
        },
        clientId: "test-producer-1",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };

    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    error? result = producer->send({topic, key, value});
    test:assertTrue(result is error);
}
