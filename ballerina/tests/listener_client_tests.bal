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

import ballerina/crypto;
import ballerina/lang.'string;
import ballerina/lang.runtime as runtime;
import ballerina/log;
import ballerina/test;

string messagesReceivedInOrder = "";
string receivedGracefulStopMessage = "";
string receivedImmediateStopMessage = "";
string saslMsg = "";
string saslIncorrectCredentialsMsg = "";
string sslMsg = "";
string detachMsg1 = "";
string detachMsg2 = "";
string incorrectEndpointMsg = "";
string receivedTimeoutConfigValue = "";
map<ByteHeaderValue?> receivedHeaders = {};

int receivedMsgCount = 0;

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerService() returns error? {
    string topic = "service-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-test-group",
        clientId: "test-listener-01"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(consumerService);
    check consumer.'start();

    runtime:sleep(3);
    test:assertEquals(receivedMessage, TEST_MESSAGE);
    check consumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerServiceInvalidUrl() returns error? {
    string topic = "consumer-service-invalid-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-invalid-url-test-group",
        clientId: "test-listener-02"
    };
    Listener consumer = check new (INCORRECT_KAFKA_URL, consumerConfiguration);
    check consumer.attach(incorrectEndpointsService);
    check consumer.'start();

    runtime:sleep(3);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check consumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testAttachDetachToClosedListener() returns error? {
    string topic = "attach-detach-closed-listener-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-invalid-test-group",
        clientId: "test-listener-03"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    error? result = consumer.gracefulStop();
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "A service must be attached before stopping the listener");
    }

    result = consumer.immediateStop();
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "A service must be attached before stopping the listener");
    }

    result = consumer.detach(incorrectEndpointsService);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "A service must be attached before detaching the listener");
    }

    result = consumer.'start();
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "A service must be attached before starting the listener");
    }

    check consumer.attach(incorrectEndpointsService);
    check consumer.'start();
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, TEST_MESSAGE);
    check consumer.immediateStop();
    check consumer.attach(incorrectEndpointsService);
    check sendMessage(TEST_MESSAGE_II.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, TEST_MESSAGE);

    error? res = consumer.detach(incorrectEndpointsService);
    test:assertTrue(res is error);
    if res is error {
        test:assertEquals(res.message(), "Error closing the Kafka consumers for service");
    }
    incorrectEndpointMsg = "";
}

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerServiceGracefulStop() returns error? {
    string topic = "listener-graceful-stop-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-graceful-stop-service-test-group",
        clientId: "test-listener-04"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(consumerGracefulStopService);
    check consumer.'start();
    runtime:sleep(3);
    test:assertEquals(receivedGracefulStopMessage, TEST_MESSAGE);

    check consumer.gracefulStop();
    check sendMessage(TEST_MESSAGE_II.toBytes(), topic);
    runtime:sleep(3);
    test:assertNotEquals(receivedGracefulStopMessage, TEST_MESSAGE_II);
}

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerServiceImmediateStop() returns error? {
    string topic = "listener-immediate-stop-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: topic,
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-immediate-stop-service-test-group",
        clientId: "test-listener-05"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(consumerImmediateStopService);
    check consumer.'start();
    runtime:sleep(3);
    test:assertEquals(receivedImmediateStopMessage, TEST_MESSAGE);

    check consumer.immediateStop();
    check sendMessage(TEST_MESSAGE_II.toBytes(), topic);
    runtime:sleep(3);
    test:assertNotEquals(receivedImmediateStopMessage, TEST_MESSAGE_II);
}

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerServiceSubscribeError() returns error? {
    string topic = "listener-subscribe-error-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: topic,
        offsetReset: OFFSET_RESET_EARLIEST,
        clientId: "test-listener-06",
        autoCommit: false
    };
    Listener|error result = trap new (DEFAULT_URL, consumerConfiguration);

    if result is error {
        string expectedErr = "The groupId of the consumer must be set to subscribe to the topics";
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    consumerConfiguration = {
        topics: [" "],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-immediate-stop-service-test-group",
        clientId: "test-listener-07"
    };

    Listener 'listener = check new (DEFAULT_URL, consumerConfiguration);
    check 'listener.attach(incorrectEndpointsService);
    error? res = 'listener.'start();
    test:assertTrue(res is error);
    if res is error {
        string expectedErr = "Error creating Kafka consumer to connect with remote broker and subscribe to " +
        "provided topics";
        test:assertEquals(res.message(), expectedErr);
    }
    check 'listener.detach(incorrectEndpointsService);
}

@test:Config {
    groups: ["listener", "service"]
}
function testListenerConfig() returns error? {
    string topic = "listener-config-test-topic";
    kafkaTopics.push(topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-test-group",
        clientId: "test-listener-08",
        pollingInterval: 3
    };

    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerConfigService);
    check serviceConsumer.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(receivedConfigMessage, TEST_MESSAGE);
    check serviceConsumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testListenerConfigError() returns error? {
    string topic = "listener-config-error-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-error-test-group-1",
        clientId: "test-listener-09",
        concurrentConsumers: -5
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    error? result = serviceConsumer.attach(consumerConfigService);
    if result is Error {
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
        clientId: "test-listener-10",
        partitionAssignmentStrategy: strategy
    };
    Listener|Error result2 = new (DEFAULT_URL, consumerConfiguration);
    if (result2 is Error) {
        string expectedErrorMsg = "Cannot connect to the kafka server: Failed to construct kafka consumer";
        test:assertEquals(result2.message(), expectedErrorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {
    groups: ["listener", "service"],
    dependsOn: [testConsumerServiceCommit]
}
function testConsumerServiceCommitOffset() returns error? {
    string topic = "listener-commit-offset-test-topic";
    kafkaTopics.push(topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-commit-offset-test-group",
        clientId: "test-listener-11",
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
    foreach int i in 0 ..< messageCount {
        check sendMessage(i.toString().toBytes(), topic);
    }
    // Wait for the service consumer to process all messages and commit offsets
    runtime:sleep(5);
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] _ = check consumer->poll(5);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    test:assertTrue(committedOffset is PartitionOffset);
    if committedOffset is PartitionOffset {
        test:assertEquals(committedOffset.offset, messageCount);
    }
    check consumer->close();
    check serviceConsumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testConsumerServiceCommit() returns error? {
    string topic = "listener-commit-test-topic";
    kafkaTopics.push(topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-commit-test-group",
        clientId: "test-listener-12",
        autoCommit: false
    };
    TopicPartition topicPartition = {
        topic: topic,
        partition: 0
    };
    Listener serviceConsumer = check new (DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerServiceWithCommit);
    check serviceConsumer.'start();
    runtime:sleep(3);
    int messageCount = 10;
    foreach int i in 0 ..< messageCount {
        check sendMessage(i.toString().toBytes(), topic);
    }
    // Wait for the service consumer to process all messages and commit offsets
    runtime:sleep(5);
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] _ = check consumer->poll(5);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    test:assertTrue(committedOffset is PartitionOffset);
    if committedOffset is PartitionOffset {
        test:assertEquals(committedOffset.offset, messageCount);
    }
    check consumer->close();
    check serviceConsumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testSaslListener() returns error? {
    string topic = "sasl-listener-test-topic";
    kafkaTopics.push(topic);

    ConsumerConfiguration consumerConfig = {
        groupId: "listener-sasl-test-group",
        clientId: "test-listener-13",
        offsetReset: "earliest",
        topics: [topic],
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new (SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(saslMsg, TEST_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testSaslListenerIncorrectCredentials() returns error? {
    string topic = "sasl-listener-incorrect-credentials-test-topic";
    AuthenticationConfiguration invalidAuthConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_INCORRECT_PASSWORD
    };

    ConsumerConfiguration consumerConfig = {
        groupId: "listener-sasl-incorrect-credentials-test-group",
        clientId: "test-listener-14",
        offsetReset: "earliest",
        topics: [topic],
        auth: invalidAuthConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new (SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerIncorrectCredentialsService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(saslIncorrectCredentialsMsg, EMPTY_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testSslListener() returns error? {
    string topic = "ssl-listener-test-topic";
    kafkaTopics.push(topic);

    ConsumerConfiguration consumerConfig = {
        groupId: "listener-sasl-test-group",
        clientId: "test-listener-15",
        offsetReset: "earliest",
        topics: [topic],
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };

    Listener saslListener = check new (SSL_URL, consumerConfig);
    check saslListener.attach(sslConsumerService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(sslMsg, TEST_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testBasicMessageOrder() returns error? {
    string topic = "message-order-test-topic";
    kafkaTopics.push(topic);
    int i = 0;
    while (i < 5) {
        string message = i.toString();
        check sendMessage(message.toBytes(), topic);
        i += 1;
    }
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "message-order-test-group",
        clientId: "test-listener-16"
    };
    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(messageOrderService);
    check consumer.'start();

    runtime:sleep(3);

    while (i < 10) {
        string message = i.toString();
        check sendMessage(message.toBytes(), topic);
        i += 1;
    }
    runtime:sleep(3);
    string expected = "0123456789";
    test:assertEquals(messagesReceivedInOrder, expected);
    check consumer.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testListenerDetach() returns error? {
    string topic1 = "listener-detach-test-topic";
    kafkaTopics.push(topic1);
    ConsumerConfiguration consumerConfiguration1 = {
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-detach-test-group",
        clientId: "test-listener-17",
        pollingInterval: 1
    };
    Listener listener1 = check new (DEFAULT_URL, consumerConfiguration1);
    check listener1.attach(listenerDetachService1);
    check listener1.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    runtime:sleep(3);
    test:assertEquals(detachMsg1, TEST_MESSAGE);

    // detach the listener and check any new messages are processed
    check listener1.detach(listenerDetachService1);
    check sendMessage(TEST_MESSAGE_II.toBytes(), topic1);
    runtime:sleep(3);
    test:assertEquals(detachMsg1, TEST_MESSAGE);

    // attach the listener and check for new messages
    check listener1.attach(listenerDetachService1);
    check listener1.'start();
    check sendMessage(TEST_MESSAGE_III.toBytes(), topic1);
    runtime:sleep(3);
    test:assertEquals(detachMsg1, TEST_MESSAGE_III);

    // detach from the current service and attach to a new service
    check listener1.detach(listenerDetachService1);
    check listener1.attach(listenerDetachService2);
    check listener1.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic1);
    runtime:sleep(3);
    test:assertEquals(detachMsg1, TEST_MESSAGE_III);
    test:assertEquals(detachMsg2, TEST_MESSAGE);
    check listener1.gracefulStop();

    // Attach a new listener to the same service and test for messages
    string topic2 = "listener2-detach-test-topic";
    kafkaTopics.push(topic2);
    ConsumerConfiguration consumerConfiguration2 = {
        topics: [topic2],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener2-detach-test-group",
        clientId: "test-listener-18"
    };
    Listener listener2 = check new (DEFAULT_URL, consumerConfiguration2);
    check listener2.attach(listenerDetachService1);
    check listener2.'start();
    detachMsg1 = "";
    check sendMessage(TEST_MESSAGE.toBytes(), topic2);
    runtime:sleep(3);
    test:assertEquals(detachMsg1, TEST_MESSAGE);
    check listener2.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testPlaintextToSecuredEndpointsListener() returns error? {
    string topic = "plaintext-secured-endpoints-listener-test-topic";
    kafkaTopics.push(topic);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "plaintext-secured-endpoints-listener-test-group",
        clientId: "test-listener-19"
    };

    Listener testListener = check new (SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();

    testListener = check new (SASL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();

    testListener = check new (SASL_SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testInvalidSecuredEndpointsListener() returns error? {
    string topic = "invalid-secured-endpoints-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "invalid-secured-endpoints-listener-test-group",
        clientId: "test-listener-20",
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };
    Listener testListener = check new (SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();

    testListener = check new (SASL_SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();

    consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "invalid-secured-endpoints-listener-test-group",
        clientId: "test-listener-21",
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };

    testListener = check new (SASL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();

    testListener = check new (SASL_SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testSslIncorrectStoresListener() returns error? {
    string topic = "ssl-incorrect-stores-listener-test-topic";
    crypto:TrustStore invalidTrustStore = {
        path: SSL_INCORRECT_TRUSTSTORE_PATH,
        password: SSL_MASTER_PASSWORD
    };

    crypto:KeyStore invalidKeyStore = {
        path: SSL_INCORRECT_KEYSTORE_PATH,
        password: SSL_MASTER_PASSWORD
    };

    SecureSocket invalidSocket = {
        cert: invalidTrustStore,
        key: {
            keyStore: invalidKeyStore,
            keyPassword: SSL_MASTER_PASSWORD
        },
        protocol: {
            name: SSL
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-incorrect-stores-listener-test-group",
        clientId: "test-listener-22",
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Listener testListener = check new (SASL_SSL_URL, consumerConfiguration);
    check testListener.attach(incorrectEndpointsService);
    check testListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(2);
    test:assertEquals(incorrectEndpointMsg, EMPTY_MESSAGE);
    check testListener.gracefulStop();
}

@test:Config {
    groups: ["listener", "service"]
}
function testSslIncorrectMasterPasswordListener() returns error? {
    string topic = "ssl-incorrect-master-password-listener-test-topic";
    crypto:TrustStore invalidTrustStore = {
        path: SSL_TRUSTSTORE_PATH,
        password: INCORRECT_SSL_MASTER_PASSWORD
    };

    crypto:KeyStore invalidKeyStore = {
        path: SSL_KEYSTORE_PATH,
        password: INCORRECT_SSL_MASTER_PASSWORD
    };

    SecureSocket invalidSocket = {
        cert: invalidTrustStore,
        key: {
            keyStore: invalidKeyStore,
            keyPassword: INCORRECT_SSL_MASTER_PASSWORD
        },
        protocol: {
            name: SSL
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-incorrect-master-password-listener-test-group",
        clientId: "test-listener-23",
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Listener|Error result = new (SSL_URL, consumerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Cannot connect to the kafka server: Failed to construct kafka consumer");
    }
}

@test:Config {
    groups: ["listener", "service"]
}
function testSslIncorrectCertPathListener() returns error? {
    string topic = "ssl-incorrect-cert-path-listener-test-topic";
    crypto:TrustStore invalidTrustStore = {
        path: SSL_TRUSTSTORE_INCORRECT_PATH,
        password: SSL_MASTER_PASSWORD
    };

    crypto:KeyStore invalidKeyStore = {
        path: SSL_KEYSTORE_INCORRECT_PATH,
        password: SSL_MASTER_PASSWORD
    };

    SecureSocket invalidSocket = {
        cert: invalidTrustStore,
        key: {
            keyStore: invalidKeyStore,
            keyPassword: SSL_MASTER_PASSWORD
        },
        protocol: {
            name: SSL
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-incorrect-cert-path-listener-test-group",
        clientId: "test-listener-24",
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Listener|Error result = new (SSL_URL, consumerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Cannot connect to the kafka server: Failed to construct kafka consumer");
    }
}

@test:Config {
    groups: ["listener", "service"]
}
function testInvalidSecurityProtocolListener() returns error? {
    string topic = "invalid-security-protocol-listener-test-topic";

    crypto:TrustStore invalidTrustStore = {
        path: SSL_TRUSTSTORE_INCORRECT_PATH,
        password: SSL_MASTER_PASSWORD
    };

    crypto:KeyStore invalidKeyStore = {
        path: SSL_KEYSTORE_INCORRECT_PATH,
        password: SSL_MASTER_PASSWORD
    };

    SecureSocket invalidSocket = {
        cert: invalidTrustStore,
        key: {
            keyStore: invalidKeyStore,
            keyPassword: SSL_MASTER_PASSWORD
        },
        protocol: {
            name: SSL
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "invalid-security-protocol-listener-test-group",
        clientId: "test-listener-25",
        secureSocket: invalidSocket,
        auth: authConfig,
        securityProtocol: PROTOCOL_SSL
    };
    Listener|Error result = new (SSL_URL, consumerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Cannot connect to the kafka server: Failed to construct kafka consumer");
    }

    consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "invalid-security-protocol-listener-test-group",
        clientId: "test-listener-26",
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SASL_SSL
    };
    result = new (SSL_URL, consumerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Cannot connect to the kafka server: Failed to construct kafka consumer");
    }

    consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "invalid-security-protocol-consumer-test-group",
        clientId: "test-listener-27",
        secureSocket: invalidSocket,
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_SSL
    };
    result = new (SSL_URL, consumerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Cannot connect to the kafka server: Failed to construct kafka consumer");
    }
}

@test:Config {
    groups: ["listener", "service"]
}
function testListenerWithPollTimeoutConfig() returns error? {
    string topic = "listener-poll-timeout-config-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);

    Service configService =
    service object {
        remote function onConsumerRecord(string[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedTimeoutConfigValue = records[i];
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: topic,
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "test-listener-group-28",
        clientId: "test-listener-28",
        pollingInterval: 2,
        pollingTimeout: 1
    };
    Listener configListener = check new (DEFAULT_URL, consumerConfiguration);
    check configListener.attach(configService);
    check configListener.'start();
    runtime:sleep(3);
    check configListener.gracefulStop();
    test:assertEquals(receivedTimeoutConfigValue, TEST_MESSAGE);
}

@test:Config {
    groups: ["listener", "service"]
}
function testListenerWithConsumerHeaders() returns error? {
    string topic = "listener-consumer-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE, topic, (), headers);

    Service headersService =
    service object {
        remote function onConsumerRecord(BytesConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedHeaders = records[i].headers;
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: topic,
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "test-listener-group-29",
        clientId: "test-listener-29"
    };
    Listener headersListener = check new (DEFAULT_URL, consumerConfiguration);
    check headersListener.attach(headersService);
    check headersListener.'start();
    runtime:sleep(3);
    check headersListener.gracefulStop();
    test:assertEquals(receivedHeaders, headers);
}

Service messageOrderService = service object {
    remote function onConsumerRecord(Caller caller, BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            messagesReceivedInOrder = messagesReceivedInOrder + message;
        }
    }
};

Service consumerService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMessage = message;
        }
    }
};

Service consumerGracefulStopService = service object {
    remote function onConsumerRecord(readonly & BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedGracefulStopMessage = message;
        }
    }
};

Service consumerImmediateStopService = service object {
    remote function onConsumerRecord(Caller caller, BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedImmediateStopMessage = message;
        }
    }
};

Service consumerServiceWithCommit = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records, Caller caller) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
        }
        check caller->'commit();
    }
};

Service consumerServiceWithCommitOffset = service object {
    remote function onConsumerRecord(readonly & BytesConsumerRecord[] records, Caller caller) returns error? {
        string topic = "listener-commit-offset-test-topic";
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMsgCount = receivedMsgCount + 1;
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

Service consumerConfigService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records, Caller caller) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedConfigMessage = message;
        }
    }
};

Service saslConsumerService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records, Caller caller) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            saslMsg = messageContent;
        }
    }
};

Service saslConsumerIncorrectCredentialsService = service object {
    remote function onConsumerRecord(Caller caller, BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            saslIncorrectCredentialsMsg = messageContent;
        }
    }
};

Service sslConsumerService = service object {
    remote function onConsumerRecord(Caller caller, readonly & BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            sslMsg = messageContent;
        }
    }
};

Service listenerDetachService1 = service object {
    remote function onConsumerRecord(Caller caller,
            BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            detachMsg1 = messageContent;
        }
    }
};

Service listenerDetachService2 = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            detachMsg2 = messageContent;
        }
    }
};

Service incorrectEndpointsService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            incorrectEndpointMsg = messageContent;
        }
    }
};
