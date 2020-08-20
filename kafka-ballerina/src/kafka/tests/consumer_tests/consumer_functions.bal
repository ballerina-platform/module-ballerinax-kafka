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
string nonExistingTopic = "non-existing-topic";
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
Producer producer = new (producerConfiguration);

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
    Consumer consumer = new (consumerConfiguration);
    var attachResult = check consumer.__attach(consumerService);
    var startResult = check consumer.__start();

    runtime:sleep(5000);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
}

@test:Config {}
function consumerFunctionsTest() returns error? {
    check sendMessage(TEST_MESSAGE, topic2);
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-functions-test-group",
        valueDeserializerType: DES_STRING,
        clientId: "test-consumer-2"
    };
    Consumer consumer = new (consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5000);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    var value = consumerRecords[0].value;
    if (value is string) {
        test:assertEquals(value, TEST_MESSAGE);
    } else {
        test:assertFail("Invalid message type received. Expected string");
    }
    var closeResult = consumer->close();
}

@test:Config {}
function consumerSubscribeUnsubscribeTest() returns error? {
    Consumer consumer = new ({
        bootstrapServers: "localhost:9092",
        groupId: "consumer-subscriber-unsubscribe-test-group",
        clientId: "test-consumer-3",
        topics: [topic1, topic2]
    });
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 2);

    var result = check consumer->unsubscribe();
    subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    var closeResult = consumer->close();
}

@test:Config {
    dependsOn: ["consumerFunctionsTest", "consumerServiceTest"]
}
function consumerSubscribeTest() returns error? {
    Consumer consumer = new ({
        bootstrapServers: "localhost:9092",
        groupId: "consumer-subscriber-test-group",
        clientId: "test-consumer-4",
        metadataMaxAgeInMillis: 2000
    });
    string[] availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 3);
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    var result = check consumer->subscribeToPattern("test.*");
    var pollResult = consumer->poll(1000); // Polling to force-update the metadata
    string[] newSubscribedTopics = check consumer->getSubscription();
    test:assertEquals(newSubscribedTopics.length(), 2);
    var closeResult = consumer->close();
}

@test:Config {}
function manualCommitTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        valueDeserializerType: DES_STRING,
        clientId: "test-consumer-4",
        autoCommit: false
    };
    Consumer consumer = new(consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString(), manualCommitTopic);
        count += 1;
    }
    var messages = consumer->poll(1000);
    TopicPartition topicPartition = {
        topic: manualCommitTopic,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: 0
    };

    var commitResult = check consumer->commitOffset([partitionOffset]);
    var committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, 0);

    var commitAllResult = check consumer->'commit();
    committedOffset = check consumer->getCommittedOffset(topicPartition);
    committedPartitionOffset = <PartitionOffset>committedOffset;
    offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, messageCount);

    int positionOffset = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(positionOffset, messageCount);
    var closeResult = consumer->close();
}

@test:Config {}
function nonExistingTopicPartitionTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        valueDeserializerType: DES_STRING,
        clientId: "test-consumer-4",
        autoCommit: false
    };
    Consumer consumer = new(consumerConfiguration);

    TopicPartition nonExistingTopicPartition = {
        topic: nonExistingTopic,
        partition: 999
    };
    var committedOffset = check consumer->getCommittedOffset(nonExistingTopicPartition);
    test:assertEquals(committedOffset, ());

    var nonExistingPositionOffset = consumer->getPositionOffset(nonExistingTopicPartition);
    test:assertTrue(nonExistingPositionOffset is ConsumerError);
    ConsumerError positionOffsetError = <ConsumerError>nonExistingPositionOffset;
    string expectedError = "Failed to retrieve position offset: You can only check the position for partitions assigned to this consumer.";
    test:assertEquals(expectedError, positionOffsetError.message());
    var closeResult = consumer->close();
}

@test:AfterSuite {}
function stopKafkaServer() returns error? {
    string parentDirectory = check getAbsoluteTestPath(TEST_DIRECTORY);
    var result = stopKafkaCluster(parentDirectory);
    if (result is error) {
        io:println(result);
    }
}

function sendMessage(string message, string topic) returns error? {
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
