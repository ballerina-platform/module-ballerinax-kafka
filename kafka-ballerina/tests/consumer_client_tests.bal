// Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

const TEST_MESSAGE = "Hello, Ballerina";
const EMPTY_MEESAGE = "";
const decimal TIMEOUT_DURATION = 5;
const decimal DEFAULT_TIMEOUT = 10;

string topic1 = "test-topic-1";
string topic2 = "test-topic-2";
string topic3 = "test-topic-3";
string topic4 = "test-topic-4";
string topic5 = "test-topic-5";
string emptyTopic = "empty-topic";
string nonExistingTopic = "non-existing-topic";
string manualCommitTopic = "manual-commit-test-topic";

string receivedMessage = "";
string receivedMessageWithCommit = "";
string receivedMessageWithCommitOffset = "";
string receivedConfigMessage = "";

int receivedMsgCount = 0;

TopicPartition nonExistingPartition = {
    topic: nonExistingTopic,
    partition: 999
};

ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3
};
Producer producer = check new (DEFAULT_URL, producerConfiguration);

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
function consumerServiceSubscribeErrorTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        clientId: "test-consumer-2"
    };
    Listener|error result = trap new (DEFAULT_URL, consumerConfiguration);

    if (result is error) {
        string expectedErr = "The groupId of the consumer must be set to subscribe to the topics";
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {}
function consumerServiceCommitTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic5],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-service-commit-test-group",
        clientId: "test-consumer-3",
        autoCommit: false
    };
    TopicPartition topicPartition = {
        topic: topic5,
        partition: 0
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerServiceWithCommit);
    check serviceConsumer.'start();

    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), topic5);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;

    test:assertEquals(offsetValue, messageCount);
}

@test:Config {
    dependsOn: [consumerServiceCommitTest]
}
function consumerServiceCommitOffsetTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic5],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-service-commit-offset-test-group",
        clientId: "test-consumer-4",
        autoCommit: false
    };
    TopicPartition topicPartition = {
        topic: topic5,
        partition: 0
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerServiceWithCommitOffset);
    check serviceConsumer.'start();

    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), topic5);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;

    test:assertEquals(offsetValue, messageCount + 10);
}

@test:Config {}
function consumerCloseTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic1],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-test-group",
            clientId: "test-consumer-7"
        };
    Consumer closeTestConsumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check closeTestConsumer->poll(5);
    var closeresult = closeTestConsumer->close();
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = closeTestConsumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerCloseWithDurationTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic1],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-with-duration-test-group",
            clientId: "test-consumer-8"
        };
    Consumer closeWithDurationTestConsumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check closeWithDurationTestConsumer->poll(5);
    var closeresult = closeWithDurationTestConsumer->close(TIMEOUT_DURATION);
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = closeWithDurationTestConsumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerCloseWithDefaultTimeoutTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic1],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-with-default-timeout-test-group",
            clientId: "test-consumer-9",
            defaultApiTimeout: DEFAULT_TIMEOUT
        };
    Consumer closeWithDefaultTimeoutTestConsumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check closeWithDefaultTimeoutTestConsumer->poll(5);
    var closeresult = closeWithDefaultTimeoutTestConsumer->close();
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = closeWithDefaultTimeoutTestConsumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerConfigTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-config-test-group",
        clientId: "test-consumer-11",
        pollingTimeout: 10,
        pollingInterval: 1,
        concurrentConsumers: 5,
        decoupleProcessing: true
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
}

@test:Config {}
function listenerConfigTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-test-group",
        clientId: "test-consumer-12",
        pollingInterval: 3
    };

    Listener serviceConsumer = check new(DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerConfigService);
    check serviceConsumer.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    runtime:sleep(7);
    test:assertEquals(receivedConfigMessage, TEST_MESSAGE);
}

@test:Config {}
function listenerConfigErrorTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-error-test-group",
        clientId: "test-consumer-13",
        concurrentConsumers: -5
    };
    Listener serviceConsumer = check new(DEFAULT_URL, consumerConfiguration);
    error? result = serviceConsumer.attach(consumerConfigService);
    if (result is error) {
        string expectedErrorMsg = "Number of Concurrent consumers should be a positive integer" +
                " value greater than zero.";
        test:assertEquals(result.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {}
function consumerFunctionsTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic2);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-functions-test-group",
        clientId: "test-consumer-14"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    byte[] value = consumerRecords[0].value;
    string message = check 'string:fromBytes(value);
    test:assertEquals(message, TEST_MESSAGE);
    check consumer->close();
}

@test:Config {}
function consumerSeekTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-test-group",
        clientId: "test-consumer-15",
        pollingTimeout: 10,
        pollingInterval: 5,
        decoupleProcessing: true,
        concurrentConsumers: 5
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

    Error? result = consumer->seekToBeginning([nonExistingPartition]);
    if (result is Error) {
        string expectedErrorMsg = "Failed to seek the consumer to the beginning: No current " +
            "assignment for partition non-existing-topic-999";
        test:assertEquals(result.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerSeekToBeginningTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-beginning-test-group",
        clientId: "test-consumer-16"
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

    Error? result = consumer->seekToBeginning([nonExistingPartition]);
    if (result is Error) {
        string expectedErrorMsg = "Failed to seek the consumer to the beginning: No current " +
            "assignment for partition non-existing-topic-999";
        test:assertEquals(result.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerSeekToEndTest() returns error? {
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic4],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-end-test-group",
        clientId: "test-consumer-17"
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

    Error? result = consumer->seekToEnd([nonExistingPartition]);
    if (result is Error) {
        string expectedErrorMsg = "Failed to seek the consumer to the end: No current " +
            "assignment for partition non-existing-topic-999";
        test:assertEquals(result.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerSeekTest, consumerSeekToBeginningTest, consumerSeekToEndTest]
}
function consumerPositionOffsetsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-position-offset-test-group",
        clientId: "test-consumer-18"
    };
    TopicPartition topicPartition = {
        topic: topic4,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    int partitionOffsetBefore = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(partitionOffsetBefore, 0, "Expected: 0. Received: " + partitionOffsetBefore.toString());
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    int partitionOffsetAfter = check consumer->getPositionOffset(topicPartition, TIMEOUT_DURATION);
    test:assertEquals(partitionOffsetAfter, 5, "Expected: 5. Received: " + partitionOffsetAfter.toString());
    check consumer->close();
    consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-position-offset-test-group",
        clientId: "test-consumer-19",
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    check sendMessage(TEST_MESSAGE.toBytes(), topic4);
    consumerRecords = check consumer->poll(5);
    partitionOffsetAfter = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(partitionOffsetAfter, 6, "Expected: 6. Received: " + partitionOffsetAfter.toString());
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerSeekTest, consumerSeekToBeginningTest, consumerSeekToEndTest]
}
function consumerBeginningOffsetsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-beginning-offsets-test-group",
        clientId: "test-consumer-20"
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
    check consumer->assign([topic1Partition, topic2Partition]);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    PartitionOffset[] partitionEndOffsets = check consumer->getBeginningOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    consumerRecords = check consumer->poll(5);
    partitionEndOffsets = check consumer->getBeginningOffsets([topic1Partition, topic2Partition], TIMEOUT_DURATION);
    test:assertEquals(partitionEndOffsets[0].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    check consumer->close();
    consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-beginning-offsets-test-group",
        clientId: "test-consumer-21",
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topic1Partition, topic2Partition]);
    consumerRecords = check consumer->poll(5);
    partitionEndOffsets = check consumer->getBeginningOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    PartitionOffset[]|Error result = consumer->getBeginningOffsets([nonExistingPartition]);
    if (result is Error) {
        string expectedErrorMsg = "Failed to retrieve offsets for the topic " +
            "partitions: Failed to get offsets by times in ";
        test:assertEquals(result.message().substring(0, 87), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
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
        clientId: "test-consumer-22"
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
    PartitionOffset[] partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 5, "Expected: 5. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition], TIMEOUT_DURATION);
    test:assertEquals(partitionEndOffsets[0].offset, 5, "Expected: 5. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    check consumer->close();
    consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-end-offset-test-group",
        clientId: "test-consumer-23",
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 5, "Expected: 5. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());

    PartitionOffset[]|Error result = consumer->getEndOffsets([nonExistingPartition]);
    if (result is Error) {
        string expectedErrorMsg = "Failed to retrieve end offsets for the " +
        "consumer: Failed to get offsets by times in ";
        test:assertEquals(result.message().substring(0, 83), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerTopicPartitionsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1, topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-topic-partitions-test-group",
        clientId: "test-consumer-24"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    TopicPartition[] topic1Partitions = check consumer->getTopicPartitions(topic1);
    test:assertEquals(topic1Partitions[0].partition, 0, "Expected: 0. Received: " + topic1Partitions[0].partition.toString());
    TopicPartition[] topic2Partitions = check consumer->getTopicPartitions(topic2);
    test:assertEquals(topic2Partitions[0].partition, 0, "Expected: 0. Received: " + topic2Partitions[0].partition.toString());
    topic1Partitions = check consumer->getTopicPartitions(topic1, TIMEOUT_DURATION);
    test:assertEquals(topic1Partitions[0].partition, 0, "Expected: 0. Received: " + topic1Partitions[0].partition.toString());
    consumer = check new (DEFAULT_URL, {
       topics: [topic1, topic2],
       offsetReset: OFFSET_RESET_EARLIEST,
       groupId: "consumer-topic-partitions-test-group",
       clientId: "test-consumer-25",
       defaultApiTimeout: DEFAULT_TIMEOUT
    });
    topic1Partitions = check consumer->getTopicPartitions(topic1);
    test:assertEquals(topic1Partitions[0].partition, 0, "Expected: 0. Received: " + topic1Partitions[0].partition.toString());
    check consumer->close();
}

@test:Config {}
function consumerPauseResumePartitionTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-pause-partition-test-group",
        clientId: "test-consumer-26"
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

    TopicPartition[] pausedPartitions = check consumer->getPausedPartitions();
    test:assertEquals(pausedPartitions[0].topic, "test-topic-1", "Expected: test-topic-1. Received: " + pausedPartitions[0].topic);

    result = consumer->resume([topicPartition]);
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerPauseResumePartitionErrorTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-pause-partition-test-group",
        clientId: "test-consumer-27"
    };
    string failingPartition = "test-topic-2-0";
    TopicPartition topicPartition1 = {
        topic: topic1,
        partition: 0
    };
    TopicPartition topicPartition2 = {
        topic: topic2,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition1]);
    Error? result = consumer->pause([topicPartition2]);
    if (result is Error) {
        string expectedErr = "Failed to pause topic partitions for the consumer: No current assignment for " +
            "partition " + failingPartition;
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    result = consumer->pause([topicPartition1]);

    result = consumer->resume([topicPartition2]);
    if (result is Error) {
        string expectedErr = "Failed to resume topic partitions for the consumer: No current assignment for " +
            "partition " + failingPartition;
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerGetAssignedPartitionsTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-get-assigned-partitions-test-group",
        clientId: "test-consumer-28"
    };
    TopicPartition topicPartition = {
        topic: topic1,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    TopicPartition[] result = check consumer->getAssignment();
    test:assertEquals(result[0].topic, "test-topic-1", "Expected: test-topic-1. Received: " + result[0].topic);
    test:assertEquals(result[0].partition, 0, "Expected: 0. Received: " + result[0].partition.toString());
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeUnsubscribeTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscriber-unsubscribe-test-group",
        clientId: "test-consumer-29",
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
        clientId: "test-consumer-30",
        metadataMaxAge: 2
    });
    string[] availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 9);
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    check consumer->subscribeWithPattern("test.*");
    ConsumerRecord[] pollResult = check consumer->poll(1); // Polling to force-update the metadata
    string[] newSubscribedTopics = check consumer->getSubscription();
    test:assertEquals(newSubscribedTopics.length(), 5);
    check consumer->close();
}

@test:Config {}
function consumerTopicsAvailableWithTimeoutTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscriber-timeout-test-group",
        clientId: "test-consumer-31",
        metadataMaxAge: 2
    });
    string[] availableTopics = check consumer->getAvailableTopics(TIMEOUT_DURATION);
    test:assertEquals(availableTopics.length(), 9);
    check consumer->close();

    consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscriber-timeout-test-group",
        clientId: "test-consumer-32",
        metadataMaxAge: 2,
        defaultApiTimeout: DEFAULT_TIMEOUT
    });
    availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 9);
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeErrorTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        clientId: "test-consumer-33"
    });
    error? result = trap consumer->subscribe([topic1]);

    if (result is error) {
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
        clientId: "test-consumer-34",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), manualCommitTopic);
        count += 1;
    }
    ConsumerRecord[] messages = check consumer->poll(1);
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
function manualCommitWithDurationTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-with-duration-test-group",
        clientId: "test-consumer-35",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] messages = check consumer->poll(1);
    int manualCommitOffset = 5;
    TopicPartition topicPartition = {
        topic: manualCommitTopic,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: manualCommitOffset
    };

    check consumer->commitOffset([partitionOffset], TIMEOUT_DURATION);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition, TIMEOUT_DURATION);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, manualCommitOffset);
    check consumer->close();
}

@test:Config {}
function manualCommitWithDefaultTimeoutTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-with-default-timeout-test-group",
        clientId: "test-consumer-36",
        autoCommit: false,
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] messages = check consumer->poll(1);
    int manualCommitOffset = 5;
    TopicPartition topicPartition = {
        topic: manualCommitTopic,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: manualCommitOffset
    };

    check consumer->commitOffset([partitionOffset]);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;
    test:assertEquals(offsetValue, manualCommitOffset);
    check consumer->close();
}

@test:Config {}
function nonExistingTopicPartitionTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        topics: [manualCommitTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-37",
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

function sendMessage(byte[] message, string topic) returns error? {
    return producer->send({ topic: topic, value: message });
}

Service consumerService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMessage = <@untainted>message;
        }
    }
};

Service consumerServiceWithCommit =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMessageWithCommit = <@untainted>message;
        }
        check caller->'commit();
    }
};

Service consumerServiceWithCommitOffset =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMsgCount = receivedMsgCount + 1;
            receivedMessageWithCommitOffset = <@untainted>message;
        }
        TopicPartition topicPartition = {
            topic: topic5,
            partition: 0
        };
        PartitionOffset partitionOffset = {
            partition: topicPartition,
            offset: receivedMsgCount
        };
        check caller->commitOffset([partitionOffset]);
    }
};

Service consumerConfigService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedConfigMessage = <@untainted>message;
        }
    }
};
