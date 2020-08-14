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

import ballerina/io;
import ballerina/runtime;
import ballerina/test;

const TEST_MESSAGE = "Hello, Ballerina";
const TEST_DIRECTORY = "consumer_tests/";

string topic1 = "test-topic-1";
string topic2 = "test-topic-2";
string manualCommitTopic = "manual-commit-test-topic";

string receivedMessage = "";

ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "basic-producer",
    acks: ACKS_ALL,
    maxBlockInMillis: 6000,
    requestTimeoutInMillis: 2000,
    valueSerializerType: SER_STRING,
    retryCount: 3
};
Producer producer = new(producerConfiguration);

@test:BeforeSuite
function startKafkaServer() returns error? {
    string yamlFilePath = "docker-compose.yaml";
    string parentDirectory = check getAbsoluteTestPath(TEST_DIRECTORY);
    var result = createKafkaCluster(parentDirectory, yamlFilePath);
    if (result is error) {
        io:println(result);
    }
}

@test:Config {}
function consumerServiceTest() returns error? {
    check sendMessage(TEST_MESSAGE, topic1);
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-service-test-group",
        valueDeserializerType: DES_STRING,
        clientId: "test-consumer-1"
    };
    Consumer consumer = new(consumerConfiguration);
    var attachResult = check consumer.__attach(consumerService);
    var startResult = check consumer.__start();

    runtime:sleep(5000);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
}

@test:Config {
    dependsOn: ["consumerServiceTest"]
}
function consumerFunctionsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-functions-test-group",
        valueDeserializerType: DES_STRING,
        clientId: "test-consumer-2"
    };
    Consumer consumer = new(consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5000);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    var value = consumerRecords[0].value;
    if (value is string) {
        test:assertEquals(value, TEST_MESSAGE);
    } else {
        test:assertFail("Invalid message type received. Expected string");
    }
}

@test:Config{}
function consumerSubscribeUnsubscribeTest() returns error? {
    Consumer kafkaConsumer = new ({
        bootstrapServers: "localhost:9092",
        groupId: "consumer-subscriber-unsubscribe-test-group",
        clientId: "test-consumer-3",
        topics: [topic1, topic2]
    });
    string[] subscribedTopics = check kafkaConsumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 2);

    var result = check kafkaConsumer->unsubscribe();
    subscribedTopics = check kafkaConsumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
}

@test:Config{}
function manualCommitTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        valueDeserializerType: DES_INT,
        clientId: "test-consumer-4",
        autoCommit: false
    };
    Consumer consumer = new(consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count, manualCommitTopic);
    }
}

@test:AfterSuite
function stopKafkaServer() returns error? {
    string parentDirectory = check getAbsoluteTestPath(TEST_DIRECTORY);
    var result = stopKafkaCluster(parentDirectory);
    if (result is error) {
        io:println(result);
    }
}

function sendMessage(anydata message, string topic) returns error? {
    return producer->send(message, topic);
}

service consumerService =
service {
    resource function onMessage(Consumer consumer, ConsumerRecord[] records) {
        foreach var kafkaRecord in records {
            var value = kafkaRecord.value;
            if (value is string) {
                receivedMessage = <@untainted>value;
            }
        }
    }
};
