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

string saslMsg = "";
string saslIncorrectCredentialsMsg = "";

@test:Config {}
function consumerServiceTest() returns error? {
    string topic = "service-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-service-test-group",
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
    string topic = "service-subscribe-error-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
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
function listenerConfigTest() returns error? {
    string topic = "listener-config-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-test-group",
        clientId: "test-consumer-12",
        pollingInterval: 3
    };

    Listener serviceConsumer = check new(DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerConfigService);
    check serviceConsumer.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(7);
    test:assertEquals(receivedConfigMessage, TEST_MESSAGE);
}

@test:Config {}
function listenerConfigErrorTest() returns error? {
    string topic = "listener-config-error-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-error-test-group-1",
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

    string strategy = "UNKNOWN_STRATEGY";
    consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-error-test-group-2",
        clientId: "test-consumer-14",
        partitionAssignmentStrategy: strategy
    };
    Listener|Error result2 = new(DEFAULT_URL, consumerConfiguration);
    if (result2 is error) {
        string expectedErrorMsg = "Cannot connect to the kafka server: Failed to construct kafka consumer";
        test:assertEquals(result2.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {
    dependsOn: [consumerServiceCommitTest]
}
function consumerServiceCommitOffsetTest() returns error? {
    string topic = "service-commit-offset-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-service-commit-offset-test-group",
        clientId: "test-consumer-4",
        autoCommit: false
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerServiceWithCommitOffset);
    check serviceConsumer.'start();

    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), topic);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;

    test:assertEquals(offsetValue, messageCount);
}

@test:Config {}
function consumerServiceCommitTest() returns error? {
    string topic = "service-commit-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-service-commit-test-group",
        clientId: "test-consumer-3",
        autoCommit: false
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerServiceWithCommit);
    check serviceConsumer.'start();

    int messageCount = 10;
    int count = 0;
    while (count < messageCount) {
        check sendMessage(count.toString().toBytes(), topic);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    PartitionOffset committedPartitionOffset = <PartitionOffset>committedOffset;
    int offsetValue = committedPartitionOffset.offset;

    test:assertEquals(offsetValue, messageCount);
}

@test:Config {}
function saslListenerTest() returns error? {
    string topic = "sasl-listener-test-topic";
    AuthenticationConfiguration authConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_PASSWORD
    };

    ConsumerConfiguration consumerConfig = {
        groupId:"listener-sasl-test-group",
        clientId: "sasl-consumer",
        offsetReset: "earliest",
        topics: [topic],
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new(SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(7);
    test:assertEquals(saslMsg, TEST_MESSAGE);
}

@test:Config {}
function saslListenerIncorrectCredentialsTest() returns error? {
    string topic = "sasl-listener-incorrect-credentials-test-topic";
    AuthenticationConfiguration authConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_INCORRECT_PASSWORD
    };

    ConsumerConfiguration consumerConfig = {
        groupId:"listener-sasl-incorrect-credentials-test-group",
        clientId: "sasl-consumer",
        offsetReset: "earliest",
        topics: [topic],
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new(SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerIncorrectCredentialsService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(7);
    test:assertEquals(saslMsg, EMPTY_MEESAGE);
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
        string topic = "service-commit-offset-test-topic";
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMsgCount = receivedMsgCount + 1;
            receivedMessageWithCommitOffset = <@untainted>message;
        }
        TopicPartition topicPartition = {
            topic: topic,
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

Service saslConsumerService =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            saslMsg = messageContent;
        }
    }
};

Service saslConsumerIncorrectCredentialsService =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            saslIncorrectCredentialsMsg = messageContent;
        }
    }
};
