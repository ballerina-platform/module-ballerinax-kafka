// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/lang.'string;
import ballerina/log;
import ballerina/lang.runtime as runtime;
import ballerina/test;

const DOCKER_COMPOSE_FILE = "docker-compose.yaml";
const TEST_MESSAGE = "Hello, Ballerina";
const TEST_DIRECTORY = "";

string topic1 = "test-topic-1";
string topic2 = "test-topic-2";
string topic3 = "test-topic-3";
string nonExistingTopic = "non-existing-topic";
string manualCommitTopic = "manual-commit-test-topic";

string receivedMessage = "";

ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "basic-producer",
    acks: ACKS_ALL,
    maxBlock: 6000,
    requestTimeout: 2000,
    retryCount: 3
};
Producer producer = checkpanic new (producerConfiguration);

@test:Config {}
function consumerServiceTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-service-test-group",
        clientId: "test-consumer-1"
    };
    Listener consumer = check new (consumerConfiguration);
    check consumer.attach(consumerService);
    check consumer.'start();

    runtime:sleep(7);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
}

@test:Config {}
function consumerFunctionsTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic2);
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-functions-test-group",
        clientId: "test-consumer-2"
    };
    Consumer consumer = check new (consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5000);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    byte[] value = consumerRecords[0].value;
    string|error message = 'string:fromBytes(value);
    if (message is string) {
        test:assertEquals(message, TEST_MESSAGE);
    } else {
        test:assertFail("Invalid message type received. Expected string");
    }
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeUnsubscribeTest() returns error? {
    Consumer consumer = check new ({
        bootstrapServers: "localhost:9092",
        groupId: "consumer-subscriber-unsubscribe-test-group",
        clientId: "test-consumer-3",
        topics: [topic1, topic2]
    });
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 2);

    check consumer->unsubscribe();
    subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest, consumerServiceTest, producerSendStringTest, manualCommitTest]
}
function consumerSubscribeTest() returns error? {
    Consumer consumer = check new ({
        bootstrapServers: "localhost:9092",
        groupId: "consumer-subscriber-test-group",
        clientId: "test-consumer-4",
        metadataMaxAge: 2000
    });
    string[] availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 5);
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    check consumer->subscribeWithPattern("test.*");
    ConsumerRecord[]|Error pollResult = consumer->poll(1000); // Polling to force-update the metadata
    string[] newSubscribedTopics = check consumer->getSubscription();
    test:assertEquals(newSubscribedTopics.length(), 3);
    check consumer->close();
}

@test:Config {}
function manualCommitTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-5",
        autoCommit: false
    };
    Consumer consumer = check new(consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), manualCommitTopic);
        count += 1;
    }
    ConsumerRecord[]|Error messages = consumer->poll(1000);
    TopicPartition topicPartition = {
        topic: manualCommitTopic,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: 0
    };

    check consumer->commitOffset([partitionOffset]);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, 0);

    check consumer->'commit();
    committedOffset = check consumer->getCommittedOffset(topicPartition);
    committedPartitionOffset = <PartitionOffset>committedOffset;
    offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, messageCount);

    int positionOffset = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(positionOffset, messageCount);
    check consumer->close();
}

@test:Config {}
function nonExistingTopicPartitionTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-6",
        autoCommit: false
    };
    Consumer consumer = check new(consumerConfiguration);

    TopicPartition nonExistingTopicPartition = {
        topic: nonExistingTopic,
        partition: 999
    };
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(nonExistingTopicPartition);
    test:assertEquals(committedOffset, ());

    int|Error nonExistingPositionOffset = consumer->getPositionOffset(nonExistingTopicPartition);
    test:assertTrue(nonExistingPositionOffset is Error);
    Error positionOffsetError = <Error>nonExistingPositionOffset;
    string expectedError = "Failed to retrieve position offset: You can only check the position for partitions assigned to this consumer.";
    test:assertEquals(expectedError, positionOffsetError.message());
    check consumer->close();
}

@test:Config {}
function producerSendStringTest() returns error? {
    Producer stringProducer = check new (producerConfiguration);
    string message = "Hello, Ballerina";
    Error? result = stringProducer->send({ topic: topic3, value: message.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());

    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic3],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-functions-test-group",
        clientId: "test-consumer-7"
    };
    Consumer stringConsumer = check new (consumerConfiguration);
    ConsumerRecord[] consumerRecords = check stringConsumer->poll(3000);
    test:assertEquals(consumerRecords.length(), 1);
    byte[] messageValue = consumerRecords[0].value;
    string|error messageConverted = 'string:fromBytes(messageValue);
    if (messageConverted is string) {
        test:assertEquals(messageConverted, TEST_MESSAGE);
    } else {
        test:assertFail("Invalid message type received. Expected string");
    }
}

@test:Config {
    dependsOn: [producerSendStringTest]
}
function producerCloseTest() returns error? {
    Producer closeTestProducer = check new (producerConfiguration);
    string message = "Test Message";
    Error? result = closeTestProducer->send({ topic: topic3, value: message.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    result = closeTestProducer->close();
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    result = closeTestProducer->send({ topic: topic3, value: message.toBytes() });
    test:assertTrue(result is error);
    error receivedErr = <error>result;
    string expectedErr = "Failed to send data to Kafka server: Cannot perform operation after producer has been closed";
    test:assertEquals(receivedErr.message(), expectedErr);
}

function sendMessage(byte[] message, string topic) returns error? {
    return producer->send({ topic: topic, value: message });
}

Service consumerService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string|error message = 'string:fromBytes(value);
            if (message is string) {
                log:printInfo("Message received: " + message);
                receivedMessage = <@untainted>message;
            }
        }
    }
};
