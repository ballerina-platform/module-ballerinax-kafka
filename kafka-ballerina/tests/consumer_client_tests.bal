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
import ballerina/test;
import ballerina/crypto;

const TEST_MESSAGE = "Hello, Ballerina";
const EMPTY_MEESAGE = "";

const decimal TIMEOUT_DURATION = 5;
const decimal DEFAULT_TIMEOUT = 10;

const string SASL_URL = "localhost:9093";
const string SSL_URL = "localhost:9094";

const string SASL_USER = "admin";
const string SASL_PASSWORD = "password";
const string SASL_INCORRECT_PASSWORD = "incorrect_password";

const string SSL_KEYSTORE_PATH = "tests/secrets/kafka.client.keystore.jks";
const string SSL_TRUSTSTORE_PATH = "tests/secrets/kafka.client.truststore.jks";
const string SSL_MASTER_PASSWORD = "password";

string emptyTopic = "empty-topic";
string nonExistingTopic = "non-existing-topic";

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
function consumerCloseTest() returns error? {
    string topic = "close-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-test-group",
            clientId: "test-consumer-7"
        };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check consumer->poll(5);
    var closeresult = consumer->close();
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = consumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerCloseWithDurationTest() returns error? {
    string topic = "close-with-duration-test-topic";
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-with-duration-test-group",
            clientId: "test-consumer-8"
        };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check consumer->poll(5);
    var closeresult = consumer->close(TIMEOUT_DURATION);
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = consumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerCloseWithDefaultTimeoutTest() returns error? {
    string topic = "close-with-default-timeout-test-topic";
    ConsumerConfiguration consumerConfiguration = {
            topics: [topic],
            offsetReset: OFFSET_RESET_EARLIEST,
            groupId: "consumer-close-with-default-timeout-test-group",
            clientId: "test-consumer-9",
            defaultApiTimeout: DEFAULT_TIMEOUT
        };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    var result = check consumer->poll(5);
    var closeresult = consumer->close();
    test:assertFalse(closeresult is error, closeresult is error ? closeresult.toString() : closeresult.toString());
    var newresult = consumer->poll(5);
    test:assertTrue(newresult is error);
    error receivedErr = <error>newresult;
    string expectedErr = "Failed to poll from the Kafka server: This consumer has already been closed.";
    test:assertEquals(receivedErr.message(), expectedErr);
}

@test:Config {}
function consumerConfigTest() returns error? {
    string topic = "consumer-config-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-config-test-group",
        clientId: "test-consumer-11",
        pollingTimeout: 10,
        pollingInterval: 1,
        concurrentConsumers: 5,
        decoupleProcessing: true
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerFunctionsTest() returns error? {
    string topic = "consumer-functions-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
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
    string topic = "consumer-seek-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
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
        topic: topic,
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
    string topic = "consumer-seek-to-beginning-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-beginning-test-group",
        clientId: "test-consumer-16"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->seekToBeginning([topicPartition]);
    consumerRecords = check consumer->poll(5);
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
function consumerSeekToEndTest() returns error? {
    string topic = "consumer-seek-to-end-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-end-test-group",
        clientId: "test-consumer-17"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
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

@test:Config {}
function consumerSeekToNegativeValueTest() returns error? {
    string topic = "consumer-seek-negative-value-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-seek-negative-value-test-group",
        clientId: "test-consumer-18"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    PartitionOffset partitionOffset = {
        partition: topicPartition,
        offset: -1
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    Error? result = consumer->seek(partitionOffset);
    if (result is Error) {
        string expectedErrorMsg = "Failed to seek the consumer: seek offset must not be a negative number";
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
    string topic = "consumer-position-offsets-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-position-offset-test-group",
        clientId: "test-consumer-18"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    int partitionOffsetBefore = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(partitionOffsetBefore, 0, "Expected: 0. Received: " + partitionOffsetBefore.toString());
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    int partitionOffsetAfter = check consumer->getPositionOffset(topicPartition, TIMEOUT_DURATION);
    test:assertEquals(partitionOffsetAfter, 2, "Expected: 2. Received: " + partitionOffsetAfter.toString());
    check consumer->close();
    consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-position-offset-test-group",
        clientId: "test-consumer-19",
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    consumerRecords = check consumer->poll(5);
    partitionOffsetAfter = check consumer->getPositionOffset(topicPartition);
    test:assertEquals(partitionOffsetAfter, 3, "Expected: 3. Received: " + partitionOffsetAfter.toString());
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerSeekTest, consumerSeekToBeginningTest, consumerSeekToEndTest]
}
function consumerBeginningOffsetsTest() returns error? {
    string topic = "consumer-beginning-offsets-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-beginning-offsets-test-group-1",
        clientId: "test-consumer-20"
    };
    TopicPartition topic1Partition = {
        topic: topic,
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
        groupId: "consumer-beginning-offsets-test-group-2",
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
    string topic = "consumer-end-offsets-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-end-offset-test-group-1",
        clientId: "test-consumer-22"
    };
    TopicPartition topic1Partition = {
        topic: topic,
        partition: 0
    };
    TopicPartition topic2Partition = {
        topic: emptyTopic,
        partition: 0
    };
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    PartitionOffset[] partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 1, "Expected: 1. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition], TIMEOUT_DURATION);
    test:assertEquals(partitionEndOffsets[0].offset, 1, "Expected: 1. Received: " + partitionEndOffsets[0].offset.toString());
    test:assertEquals(partitionEndOffsets[1].offset, 0, "Expected: 0. Received: " + partitionEndOffsets[1].offset.toString());
    check consumer->close();
    consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-end-offset-test-group-2",
        clientId: "test-consumer-23",
        defaultApiTimeout: DEFAULT_TIMEOUT
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    partitionEndOffsets = check consumer->getEndOffsets([topic1Partition, topic2Partition]);
    test:assertEquals(partitionEndOffsets[0].offset, 1, "Expected: 1. Received: " + partitionEndOffsets[0].offset.toString());
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
    string topic1 = "consumer-topic-partitions-test-topic-1";
    string topic2 = "consumer-topic-partitions-test-topic-2";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1, topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-topic-partitions-test-group-1",
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
       groupId: "consumer-topic-partitions-test-group-2",
       clientId: "test-consumer-25",
       defaultApiTimeout: DEFAULT_TIMEOUT
    });
    topic1Partitions = check consumer->getTopicPartitions(topic1);
    test:assertEquals(topic1Partitions[0].partition, 0, "Expected: 0. Received: " + topic1Partitions[0].partition.toString());
    check consumer->close();
}

@test:Config {}
function consumerPauseResumePartitionTest() returns error? {
    string topic = "consumer-pause-resume-partition-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-pause-partition-test-group",
        clientId: "test-consumer-26"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    Error? result = consumer->pause([topicPartition]);
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 0, "Expected: 0. Received: " + consumerRecords.length().toString());

    TopicPartition[] pausedPartitions = check consumer->getPausedPartitions();
    test:assertEquals(pausedPartitions[0].topic, topic, "Expected: consumer-pause-resume-partition-test-topic. Received: " + pausedPartitions[0].topic);

    result = consumer->resume([topicPartition]);
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {}
function consumerPauseResumePartitionErrorTest() returns error? {
    string topic1 = "consumer-pause-resume-partition-error-test-topic-1";
    string topic2 = "consumer-pause-resume-partition-error-test-topic-2";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-pause-partition-error-test-group",
        clientId: "test-consumer-27"
    };
    string failingPartition = topic2 + "-0";
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
function consumerAssignToEmptyTopicTest() returns error? {
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-assign-empty-topic-test-group",
        clientId: "test-consumer-28"
    };
    TopicPartition topicPartition = {
        topic: "",
        partition: 0
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    Error? result = consumer->assign([topicPartition]);
    if (result is Error) {
        string expectedErr = "Failed to assign topics for the consumer: Topic partitions to assign to cannot have null or empty topic";
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerGetAssignedPartitionsTest() returns error? {
    string topic = "consumer-assigned-partitions-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-get-assigned-partitions-test-group",
        clientId: "test-consumer-28"
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };

    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer->assign([topicPartition]);
    TopicPartition[] result = check consumer->getAssignment();
    test:assertEquals(result[0].topic, topic, "Expected: " + topic + ". Received: " + result[0].topic);
    test:assertEquals(result[0].partition, 0, "Expected: 0. Received: " + result[0].partition.toString());
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeUnsubscribeTest() returns error? {
    string topic1 = "consumer-subscribe-unsubscribe-test-topic-1";
    string topic2 = "consumer-subscribe-unsubscribe-test-topic-2";
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
    test:assertEquals(availableTopics.length(), 27);
    string[] subscribedTopics = check consumer->getSubscription();
    test:assertEquals(subscribedTopics.length(), 0);
    check consumer->subscribeWithPattern("consumer.*");
    ConsumerRecord[] pollResult = check consumer->poll(1); // Polling to force-update the metadata
    string[] newSubscribedTopics = check consumer->getSubscription();
    test:assertEquals(newSubscribedTopics.length(), 10);
    check consumer->close();
}

@test:Config {}
function consumerSubscribeWithPatternToClosedConsumerTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscribe-closed-consumer-group",
        clientId: "test-consumer-31",
        metadataMaxAge: 2
    });
    check consumer->close();
    Error? result = consumer->subscribeWithPattern("consumer.*");
    if result is Error {
        string errorMsg = "Failed to subscribe to the topics: This consumer has already been closed.";
        test:assertEquals(result.message(), errorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {}
function consumerSubscribeToEmptyTopicTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-subscribe-empty-topic-test-group",
        clientId: "test-consumer-32",
        metadataMaxAge: 2
    });
    Error? result = consumer->subscribe([" "]);
    if result is Error {
        string errorMsg = "Failed to subscribe to the provided topics: Topic collection to subscribe to cannot contain " +
        "null or empty topic";
        test:assertEquals(result.message(), errorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config {}
function consumerTopicsAvailableWithTimeoutTest() returns error? {
    Consumer consumer = check new (DEFAULT_URL, {
        groupId: "consumer-topics-available-timeout-test-group-1",
        clientId: "test-consumer-33",
        metadataMaxAge: 2
    });
    string[] availableTopics = check consumer->getAvailableTopics(TIMEOUT_DURATION);
    test:assertEquals(availableTopics.length(), 29);
    check consumer->close();

    consumer = check new (DEFAULT_URL, {
        groupId: "consumer-topics-available-timeout-test-group-2",
        clientId: "test-consumer-32",
        metadataMaxAge: 2,
        defaultApiTimeout: DEFAULT_TIMEOUT
    });
    availableTopics = check consumer->getAvailableTopics();
    test:assertEquals(availableTopics.length(), 29);
    check consumer->close();
}

@test:Config {
    dependsOn: [consumerFunctionsTest]
}
function consumerSubscribeErrorTest() returns error? {
    string topic = "consumer-subsribe-error-test-topic";
    Consumer consumer = check new (DEFAULT_URL, {
        clientId: "test-consumer-33"
    });
    error? result = trap consumer->subscribe([topic]);

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
    string topic = "manual-commit-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-test-group",
        clientId: "test-consumer-34",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), topic);
        count += 1;
    }
    ConsumerRecord[] messages = check consumer->poll(1);
    TopicPartition topicPartition = {
        topic: topic,
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
    string topic = "manual-commit-with-duration-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-manual-commit-with-duration-test-group",
        clientId: "test-consumer-35",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] messages = check consumer->poll(1);
    int manualCommitOffset = 5;
    TopicPartition topicPartition = {
        topic: topic,
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
    string topic = "manual-commit-with-default-timeout-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
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
        topic: topic,
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
function nonExistingTopicPartitionOffsetsTest() returns error? {
    string existingTopic = "existing-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [existingTopic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-non-existing-topic-partitions-test-group",
        clientId: "test-consumer-37",
        autoCommit: false
    };
    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);

    PartitionOffset? committedOffset = check consumer->getCommittedOffset(nonExistingPartition);
    test:assertEquals(committedOffset, ());

    int|Error nonExistingPositionOffset = consumer->getPositionOffset(nonExistingPartition);
    test:assertTrue(nonExistingPositionOffset is Error);
    Error positionOffsetError = <Error>nonExistingPositionOffset;
    string expectedError = "Failed to retrieve position offset: You can only check the position for partitions assigned to this consumer.";
    test:assertEquals(expectedError, positionOffsetError.message());
    check consumer->close();
}

@test:Config{}
function saslConsumerTest() returns error? {
    string topic = "sasl-consumer-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    AuthenticationConfiguration authConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_PASSWORD
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-sasl-test-group",
        clientId: "test-consumer-40",
        autoCommit: false,
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };
    Consumer consumer = check new(SASL_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config{}
function saslConsumerIncorrectCredentialsTest() returns error? {
    string topic = "sasl-consumer-incorrect-credentials-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    AuthenticationConfiguration authConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_INCORRECT_PASSWORD
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-sasl-incorrect-credentials-test-group",
        clientId: "test-consumer-41",
        autoCommit: false,
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };
    Consumer consumer = check new(SASL_URL, consumerConfiguration);
    ConsumerRecord[]|Error result = consumer->poll(5);
    if result is Error {
        string errorMsg = "Failed to poll from the Kafka server: Authentication failed: Invalid username or password";
        test:assertEquals(result.message(), errorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check consumer->close();
}

@test:Config{}
function consumerAdditionalPropertiesTest() returns error? {
    string topic = "consumer-additional-properties-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    map<string> propertyMap = {
        "enable.auto.commit": "false"
    };
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-additional-properties-test-group",
        clientId: "test-consumer-42",
        additionalProperties: propertyMap
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };

    Consumer consumer = check new(DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] result = check consumer->poll(5);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    test:assertEquals(committedOffset, ());
    check consumer->close();
}

@test:Config {}
function sslConsumerTest() returns error? {
    string topic = "ssl-consumer-test-topic";
    crypto:TrustStore trustStore = {
        path: SSL_TRUSTSTORE_PATH,
        password: SSL_MASTER_PASSWORD
    };

    crypto:KeyStore keyStore = {
        path: SSL_KEYSTORE_PATH,
        password: SSL_MASTER_PASSWORD
    };

    SecureSocket socket = {
        cert: trustStore,
        key: {
            keyStore: keyStore,
            keyPassword: SSL_MASTER_PASSWORD
        },
        protocol: {
            name: SSL
        }
    };
    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-consumer-test-group",
        clientId: "test-consumer-40",
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };
    Consumer consumer = check new (SSL_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

function sendMessage(byte[] message, string topic) returns error? {
    return producer->send({ topic: topic, value: message });
}
