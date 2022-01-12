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
import ballerina/crypto;

string messagesReceivedInOrder = "";
string receivedGracefulStopMessage = "";
string receivedImmediateStopMessage = "";
string saslMsg = "";
string saslIncorrectCredentialsMsg = "";
string sslMsg = "";
string detachMsg1 = "";
string detachMsg2 = "";
string incorrectEndpointMsg = "";

int receivedMsgCount = 0;

@test:Config {}
function consumerServiceTest() returns error? {
    string topic = "service-test-topic";
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

@test:Config {}
function consumerServiceInvalidUrlTest() returns error? {
    string topic = "consumer-service-invalid-topic";
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

@test:Config {}
function attachDetachToClosedListenerTest() returns error? {
    string topic = "attach-detach-closed-listener-topic";
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
        test:assertEquals(result.message(), "A service must be attaced before stopping the listener");
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

@test:Config {}
function consumerServiceGracefulStopTest() returns error? {
    string topic = "listener-graceful-stop-test-topic";
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

@test:Config {}
function consumerServiceImmediateStopTest() returns error? {
    string topic = "listener-immediate-stop-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
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

@test:Config {}
function consumerServiceSubscribeErrorTest() returns error? {
    string topic = "listener-subscribe-error-test-topic";
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        clientId: "test-listener-06"
    };
    Listener|error result = trap new (DEFAULT_URL, consumerConfiguration);

    if result is Error {
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
    result = new (DEFAULT_URL, consumerConfiguration);

    if result is Error {
        string expectedErr = "Failed to subscribe to the provided topics: Topic collection to subscribe to " +
        "cannot contain null or empty topic";
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
        clientId: "test-listener-08",
        pollingInterval: 3
    };

    Listener serviceConsumer = check new(DEFAULT_URL, consumerConfiguration);
    check serviceConsumer.attach(consumerConfigService);
    check serviceConsumer.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(receivedConfigMessage, TEST_MESSAGE);
    check serviceConsumer.gracefulStop();
}

@test:Config {}
function listenerConfigErrorTest() returns error? {
    string topic = "listener-config-error-test-topic";
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "listener-config-error-test-group-1",
        clientId: "test-listener-09",
        concurrentConsumers: -5
    };
    Listener serviceConsumer = check new(DEFAULT_URL, consumerConfiguration);
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
    Listener|Error result2 = new(DEFAULT_URL, consumerConfiguration);
    if (result2 is Error) {
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
    string topic = "listener-commit-offset-test-topic";
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
    int count = 0;
    while count < messageCount {
        check sendMessage(count.toString().toBytes(), topic);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] _ = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    test:assertTrue(committedOffset is PartitionOffset);
    if committedOffset is PartitionOffset {
        test:assertEquals(committedOffset.offset, messageCount);
    }
    check consumer->close();
    check serviceConsumer.gracefulStop();
}

@test:Config {}
function consumerServiceCommitTest() returns error? {
    string topic = "listener-commit-test-topic";
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

    int messageCount = 10;
    int count = 0;
    while count < messageCount {
        check sendMessage(count.toString().toBytes(), topic);
        count += 1;
    }
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] _ = check consumer->poll(1);
    PartitionOffset? committedOffset = check consumer->getCommittedOffset(topicPartition);
    test:assertTrue(committedOffset is PartitionOffset);
    if committedOffset is PartitionOffset {
        test:assertEquals(committedOffset.offset, messageCount);
    }
    check consumer->close();
    check serviceConsumer.gracefulStop();
}

@test:Config {}
function saslListenerTest() returns error? {
    string topic = "sasl-listener-test-topic";

    ConsumerConfiguration consumerConfig = {
        groupId:"listener-sasl-test-group",
        clientId: "test-listener-13",
        offsetReset: "earliest",
        topics: [topic],
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new(SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(saslMsg, TEST_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {}
function saslListenerIncorrectCredentialsTest() returns error? {
    string topic = "sasl-listener-incorrect-credentials-test-topic";
    AuthenticationConfiguration invalidAuthConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_INCORRECT_PASSWORD
    };

    ConsumerConfiguration consumerConfig = {
        groupId:"listener-sasl-incorrect-credentials-test-group",
        clientId: "test-listener-14",
        offsetReset: "earliest",
        topics: [topic],
        auth: invalidAuthConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Listener saslListener = check new(SASL_URL, consumerConfig);
    check saslListener.attach(saslConsumerIncorrectCredentialsService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(saslMsg, EMPTY_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {}
function sslListenerTest() returns error? {
    string topic = "ssl-listener-test-topic";

    ConsumerConfiguration consumerConfig = {
        groupId:"listener-sasl-test-group",
        clientId: "test-listener-15",
        offsetReset: "earliest",
        topics: [topic],
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };

    Listener saslListener = check new(SSL_URL, consumerConfig);
    check saslListener.attach(sslConsumerService);
    check saslListener.'start();
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    runtime:sleep(3);
    test:assertEquals(sslMsg, TEST_MESSAGE);
    check saslListener.gracefulStop();
}

@test:Config {}
function basicMessageOrderTest() returns error? {
    string topic = "message-order-test-topic";
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

@test:Config {}
function listenerDetachTest() returns error? {
    string topic1 = "listener-detach-test-topic";
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

@test:Config {}
function plaintextToSecuredEndpointsListenerTest() returns error? {
    string topic = "plaintext-secured-endpoints-listener-test-topic";

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

@test:Config {}
function invalidSecuredEndpointsListenerTest() returns error? {
    string topic = "invalid-secured-endpoints-listener-test-topic";

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

@test:Config {}
function sslIncorrectStoresListenerTest() returns error? {
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

@test:Config {}
function sslIncorrectMasterPasswordListenerTest() returns error? {
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

@test:Config {}
function sslIncorrectCertPathListenerTest() returns error? {
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

@test:Config {}
function invalidSecurityProtocolListenerTest() returns error? {
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


Service messageOrderService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            messagesReceivedInOrder = messagesReceivedInOrder + message;
        }
    }
};

Service consumerService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMessage = message;
        }
    }
};

Service consumerGracefulStopService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedGracefulStopMessage = message;
        }
    }
};

Service consumerImmediateStopService =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedImmediateStopMessage = message;
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
            receivedMessageWithCommit = message;
        }
        check caller->'commit();
    }
};

Service consumerServiceWithCommitOffset =
service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        string topic = "listener-commit-offset-test-topic";
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check 'string:fromBytes(value);
            log:printInfo("Message received: " + message);
            receivedMsgCount = receivedMsgCount + 1;
            receivedMessageWithCommitOffset = message;
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
            receivedConfigMessage = message;
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

Service sslConsumerService =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            sslMsg = messageContent;
        }
    }
};

Service listenerDetachService1 =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            detachMsg1 = messageContent;
        }
    }
};

Service listenerDetachService2 =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            detachMsg2 = messageContent;
        }
    }
};

Service incorrectEndpointsService =
service object {
    remote function onConsumerRecord(Caller caller,
                                ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string messageContent = check 'string:fromBytes(consumerRecord.value);
            log:printInfo(messageContent);
            incorrectEndpointMsg = messageContent;
        }
    }
};
