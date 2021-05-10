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
string topic4 = "test-topic-4";
string emptyTopic = "empty-topic";
string nonExistingTopic = "non-existing-topic";
string manualCommitTopic = "manual-commit-test-topic";

string receivedMessage = "";

ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3
};
Producer producer = checkpanic new (DEFAULT_URL, producerConfiguration);

@test:Config {}
function consumerServiceTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-service-test-group",
        clientId: "test-consumer-1"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(consumerService);
    check consumer.'start();

    runtime:sleep(7);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
}

@test:Config {}
function consumerCloseTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic1],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-test-group",
            clientId: "test-consumer-2"
        };
    Consumer closeTestConsumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check closeTestConsumer->poll(5);
    //test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    var closeresult = closeTestConsumer->close();
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = closeTestConsumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerFunctionsTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic2);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-functions-test-group",
        clientId: "test-consumer-3"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
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

@test:Config {}
function consumerSeekTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-test-group",
        clientId: "test-consumer-4"
    };
    TopicPartition topicPartition = {
        topic: topic4,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: 1
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    check consumer->seek(partitionOffset);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerSeekToBeginningTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-beginning-test-group",
        clientId: "test-consumer-5"
    };
    TopicPartition topicPartition = {
        topic: topic4,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 3, "Expected: 3. Received: " + consumerRecords.length().toString());
    check consumer->seekToBeginning([topicPartition]);
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 3, "Expected: 3. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerSeekToEndTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-end-test-group",
        clientId: "test-consumer-6"
    };
    TopicPartition topicPartition = {
        topic: topic4,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 4, "Expected: 4. Received: " + consumerRecords.length().toString());
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    check consumer->seekToEnd([topicPartition]);
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 0, "Expected: 0. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerSeekTest, consumerSeekToBeginningTest, consumerSeekToEndTest]
}
function consumerPositionOffsetsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-position-offset-test-group",
        clientId: "test-consumer-7"
    };
    TopicPartition topicPartition = {
        topic: topic4,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    int|Error partitionOffsetBefore = consumer->getPositionOffset(topicPartition);
    if (partitionOffsetBefore is error) {
        test:assertFail(msg = "Invalid offset received");
    } else {
        test:assertEquals(partitionOffsetBefore, 0, "Expected: 0. Received: " + partitionOffsetBefore.toString());
    }
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    int|Error partitionOffsetAfter = consumer->getPositionOffset(topicPartition);
    if (partitionOffsetAfter is error) {
        test:assertFail(msg = "Invalid offset received");
    } else {
        test:assertEquals(partitionOffsetAfter, 5, "Expected: 0. Received: " + partitionOffsetAfter.toString());
    }
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerSeekTest, consumerSeekToBeginningTest, consumerSeekToEndTest]
}
function consumerEndOffsetsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-end-offset-test-group",
        clientId: "test-consumer-8"
    };
    TopicPartition topic1Partition = {
        topic: topic4,
        partition: 0
    };
    TopicPartition topic2Partition = {
        topic: emptyTopic,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    PartitionOffset[]|Error partitionEndOffsets = consumer->getEndOffsets([topic1Partition, topic2Partition]);
    if (partitionEndOffsets is error) {
        test:assertFail(msg = "Invalid offset received");
    } else {
        test:assertEquals(partitionEndOffsets[0].offset, 5, "Expected: 5. Received: " + partitionEndOffsets[0].offset.toString());
        test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    }
    check consumer->close();
}

@test:Config {}
function consumerTopicPartitionsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1, topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-topic-partitions-test-group",
        clientId: "test-consumer-9"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    TopicPartition[]|Error topic1Partitions = consumer->getTopicPartitions(topic1);
    if (topic1Partitions is error) {
        test:assertFail(msg = "Invalid offset received");
    } else {
        test:assertEquals(topic1Partitions[0].partition, 0, "Expected: 0. Received: " + topic1Partitions[0].partition.toString());
    }
    TopicPartition[]|Error topic2Partitions = consumer->getTopicPartitions(topic2);
    if (topic2Partitions is error) {
        test:assertFail(msg = "Invalid offset received");
    } else {
        test:assertEquals(topic2Partitions[0].partition, 0, "Expected: 0. Received: " + topic2Partitions[0].partition.toString());
    }
    check consumer->close();
}

@test:Config {}
function consumerPauseResumePartitionTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-pause-partition-test-group",
        clientId: "test-consumer-10"
    };
    TopicPartition topicPartition = {
        topic: topic1,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    Error? result = consumer->pause([topicPartition]);
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 0, "Expected: 0. Received: " + consumerRecords.length().toString());

    TopicPartition[]|Error pausedPartitions = consumer->getPausedPartitions();
    if (pausedPartitions is error) {
        test:assertFail(msg = "Invalid result received");
    } else {
        test:assertEquals(pausedPartitions[0].topic, "test-topic-1", "Expected: test-topic-1. Received: " + pausedPartitions[0].topic);
    }

    result = consumer->resume([topicPartition]);
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerGetAssignedPartitionsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-get-assigned-partitions-test-group",
        clientId: "test-consumer-11"
    };
    TopicPartition topicPartition = {
        topic: topic1,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    TopicPartition[]|Error result = check consumer->getAssignment();
    if (result is error) {
        test:assertFail(msg = "Invalid result received");
    } else {
        test:assertEquals(result[0].topic, "test-topic-1", "Expected: test-topic-1. Received: " + result[0].topic);
    }
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeUnsubscribeTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscriber-unsubscribe-test-group",
        clientId: "test-consumer-12",
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
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscriber-test-group",
        clientId: "test-consumer-13",
        metadataMaxAge: 2
    });
    string[] availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 7);
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    check consumer->subscribeWithPattern("test.*");
    ConsumerRecord[]|Error pollResult = consumer->poll(1); // Polling to force-update the metadata
    string[] newSubscribedTopics = check consumer->getSubscription();
    test:assertEquals(newSubscribedTopics.length(), 4);
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeErrorTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        clientId: "test-consumer-14"
    });
    (Error|error)? result = trap consumer->subscribe([topic1]);

    if (result is Error) {
        string expectedErr = "The groupId of the consumer must be set to subscribe to the topics";
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function manualCommitTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-15",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), manualCommitTopic);
        count += 1;
    }
    ConsumerRecord[]|Error messages = consumer->poll(1);
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
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-16",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);

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
    Producer stringProducer = check new (DEFAULT_URL, producerConfiguration);
    string message = "Hello, Ballerina";
    Error? result = stringProducer->send({ topic: topic3, value: message.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic3],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-functions-test-group",
        clientId: "test-consumer-17"
    };
    Consumer stringConsumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check stringConsumer->poll(3);
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
    Producer closeTestProducer = check new (DEFAULT_URL, producerConfiguration);
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
