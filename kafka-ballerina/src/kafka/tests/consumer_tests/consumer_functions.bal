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
import ballerina/kafka;
import ballerina/runtime;
import ballerina/test;

const TEST_MESSAGE = "Hello, Ballerina";

string topic1 = "test-topic-1";
string topic2 = "test-topic-2";

string receivedMessage = "";

kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "basic-producer",
    acks: kafka:ACKS_ALL,
    maxBlock: 6000,
    requestTimeoutInMillis: 2000,
    valueSerializerType: kafka:SER_STRING,
    retryCount: 3
};

kafka:ConsumerConfiguration consumerConfiguration = {
    bootstrapServers: "localhost:9092",
    topics: [topic1, topic2],
    groupId: "test-group-1",
    clientId: "test-consumer-1"
};

@test:BeforeSuite
function startKafkaServer() returns error? {
    string yamlFilePath = "docker-compose.yaml";
    string parentDirectory = check getAbsoluteTestPath("consumer_tests/");
    var result = createKafkaCluster(parentDirectory, yamlFilePath);
    if (result is error) {
        io:println(result);
    }
}

@test:Config {
    dependsOn: ["testProducer"]
}
function testConsumer() returns error? {
    kafka:Consumer consumer = new(consumerConfiguration);
    var attachResult = check consumer.__attach(consumerService);
    var startResult = check consumer.__start();

    kafka:Producer producer = new(producerConfiguration);
    var sendResult = check producer->send(TEST_MESSAGE, topic2);
    runtime:sleep(10000);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
}

@test:Config {}
function testProducer() returns error? {
    kafka:Producer producer = new(producerConfiguration);
    return producer->send(TEST_MESSAGE, topic1);
}

@test:Config{}
function testTestUnsubscribe() returns error? {
    kafka:Consumer kafkaConsumer = new ({
        bootstrapServers: "localhost:9092",
        groupId: "test-group",
        clientId: "subscription-consumer",
        topics: [topic1, topic2]
    });
    string[] subscribedTopics = check kafkaConsumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 2);

    var result = check kafkaConsumer->unsubscribe();
    subscribedTopics = check kafkaConsumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
}

@test:AfterSuite
function stopKafkaServer() returns error? {
    string parentDirectory = check getAbsoluteTestPath("consumer_tests/");
    var result = stopKafkaCluster(parentDirectory);
    if (result is error) {
        io:println(result);
    }
}

service consumerService =
service {
    resource function onMessage(kafka:Consumer consumer, kafka:ConsumerRecord[] records) {
        io:println("HJGADKJFGK");
        foreach var kafkaRecord in records {
            var value = kafkaRecord.value;
            if (value is string) {
                receivedMessage = <@untainted>value;
            }
        }
    }
};
