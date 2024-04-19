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
import ballerina/io;
import ballerina/crypto;

string MESSAGE_KEY = "TEST-KEY";

@test:Config{enable: true}
function producerInitTest() returns error? {
    ProducerConfiguration producerConfiguration1 = {
        clientId: "test-producer-02",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };
    ProducerConfiguration producerConfiguration2 = {
        clientId: "test-producer-03",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        transactionalId: "prod-id-1",
        enableIdempotence: true
    };
    ProducerConfiguration producerConfiguration3 = {
        clientId: "test-producer-04",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        transactionalId: "prod-id-2"
    };
    Producer result1 = check new (DEFAULT_URL, producerConfiguration1);
    Producer result2 = check new (DEFAULT_URL, producerConfiguration2);
    check result1->close();
    check result2->close();

    Producer|Error result3 = new (DEFAULT_URL, producerConfiguration3);
    if result3 is Error {
        string expectedErr = "configuration enableIdempotence must be set to true to enable " +
            "transactional producer";
         test:assertEquals(result3.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }

    Producer|Error result4 = new (INVALID_URL, producerConfiguration1);
    if result4 is Error {
        string expectedErr = "Failed to initialize the producer: " +
            "No resolvable bootstrap urls given in bootstrap.servers";
        test:assertEquals(result4.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {enable: true}
function producerSendStringTest() returns error? {
    string topic = "send-string-test-topic";
    kafkaTopics.push(topic);
    Producer stringProducer = check new (DEFAULT_URL, producerConfiguration);
    string message = "Hello, Ballerina";
    Error? result = stringProducer->send({ topic: topic, value: message.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    result = stringProducer->send({ topic: topic, value: message.toBytes(), key: MESSAGE_KEY.toBytes() });
    check stringProducer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-send-string-test-group",
        clientId: "test-consumer-46"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 2);
    byte[] messageValue = consumerRecords[0].value;
    string messageConverted = check 'string:fromBytes(messageValue);
    test:assertEquals(messageConverted, TEST_MESSAGE);
    check consumer->close();
}

@test:Config {enable: true}
function producerKeyTypeMismatchErrorTest() returns error? {
    string topic = "key-type-mismatch-error-test-topic";
    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    string message = "Hello, Ballerina";
    error? result = trap sendByteArrayValues(producer, message.toBytes(), topic, [], MESSAGE_KEY, 0, (), SER_BYTE_ARRAY);
    if result is error {
        string expectedErr = "Invalid type found for Kafka key. Expected key type: 'byte[]'.";
        test:assertEquals(result.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check producer->close();
}

@test:Config {
    enable: true,
    dependsOn: [producerSendStringTest]
}
function producerCloseTest() returns error? {
    string topic = "producer-close-test-topic";
    kafkaTopics.push(topic);
    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    string message = "Test Message";
    Error? result = producer->send({ topic: topic, value: message.toBytes() });
    test:assertFalse(result is Error, result is Error ? result.toString() : result.toString());
    result = producer->close();
    test:assertFalse(result is Error, result is Error ? result.toString() : result.toString());
    result = producer->send({ topic: topic, value: message.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        string expectedErr = "Failed to send data to Kafka server: Cannot perform operation after producer has been closed";
        test:assertEquals(result.message(), expectedErr);
    }
    check producer->close();
}

@test:Config {enable: true}
function producerFlushTest() returns error? {
    string topic = "producer-flush-test-topic";
    kafkaTopics.push(topic);
    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->'flush();
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-flush-test-group",
        clientId: "test-consumer-47"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals('string:fromBytes(consumerRecords[0].value), TEST_MESSAGE);
    check consumer->close();
}

@test:Config {enable: true}
function producerGetTopicPartitionsTest() returns error? {
    string topic = "get-topic-partitions-test-topic";
    kafkaTopics.push(topic);
    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    TopicPartition[] topicPartitions = check producer->getTopicPartitions(topic);
    test:assertEquals(topicPartitions[0].partition, 0, "Expected: 0. Received: " + topicPartitions[0].partition.toString());
    check producer->close();
}

@test:Config {enable: true}
function producerGetTopicPartitionsErrorTest() returns error? {
    string topic = "get-topic-partitions-error-test-topic";
    Producer producer = check new (INCORRECT_KAFKA_URL, producerConfiguration);
    TopicPartition[]|Error result = producer->getTopicPartitions(topic);
    if result is error {
        string expectedErr = "Failed to fetch partitions from the producer Topic " +
                                topic + " not present in metadata after ";
        test:assertEquals(result.message().substring(0, expectedErr.length()), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    check producer->close();
}

@test:Config {enable: true}
function transactionalProducerTest() returns error? {
    string topic = "transactional-producer-test-topic";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-05",
        acks: "all",
        retryCount: 3,
        enableIdempotence: true,
        transactionalId: "test-transactional-id"
    };
    Producer producer = check new (DEFAULT_URL, producerConfigs);
    transaction {
        check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
        check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
        check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
        var commitResult = commit;
        if commitResult is () {
            io:println("Commit successful");
        } else {
            test:assertFail(msg = "Commit Failed");
        }
    }
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-transactional-test-group",
        clientId: "test-consumer-48"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 3, "Expected: 3. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function transactionalProducerWithAbortTest() returns error? {
    string topic = "rollback-producer-test-topic";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-05",
        acks: "all",
        retryCount: 3,
        enableIdempotence: true,
        transactionalId: "producer-abort-transactional-id"
    };
    Producer producer = check new (DEFAULT_URL, producerConfigs);
    do {
        transaction {
            check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
            check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
            check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes(), partition: 0});
            check failTransaction();
            check commit;
        }
    } on fail var e {
        io:println(e.toString());
    }
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "committed-transactional-test-group",
        clientId: "test-consumer-49",
        isolationLevel: ISOLATION_COMMITTED
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 0, "Expected: 0. Received: " + consumerRecords.length().toString());
    check consumer->close();

    consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "uncommitted-transactional-test-group",
        clientId: "test-consumer-50",
        isolationLevel: ISOLATION_UNCOMMITTED
    };
    consumer = check new (DEFAULT_URL, consumerConfiguration);
    consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 3, "Expected: 3. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

isolated function failTransaction() returns error {
    return error("Fail!");
}

@test:Config{enable: true}
function saslProducerTest() returns error? {
    string topic = "sasl-producer-test-topic";
    kafkaTopics.push(topic);

    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-06",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Producer kafkaProducer = check new (SASL_URL, producerConfigs);

    Error? result = kafkaProducer->send({topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    check kafkaProducer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "sasl-producer-test-group",
        clientId: "test-consumer-49"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config{enable: true}
function saslProducerIncorrectCredentialsTest() returns error? {
    string topic = "sasl-producer-incorrect-credentials-test-topic";
    kafkaTopics.push(topic);
    AuthenticationConfiguration invalidAuthConfig = {
        mechanism: AUTH_SASL_PLAIN,
        username: SASL_USER,
        password: SASL_INCORRECT_PASSWORD
    };

    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-07",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        auth: invalidAuthConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Producer kafkaProducer = check new (SASL_URL, producerConfigs);

    Error? result = kafkaProducer->send({topic: topic, value: TEST_MESSAGE.toBytes() });
    if result is Error {
        string errorMsg = "Failed to send data to Kafka server: Authentication failed: Invalid username or password";
        test:assertEquals(result.message(), errorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }

    kafkaProducer = check new (SSL_URL, producerConfigs);
    result = kafkaProducer->send({topic: topic, value: TEST_MESSAGE.toBytes() });
    if result is Error {
        string errorMsg = "Failed to send data to Kafka server: Topic sasl-producer-incorrect-credentials-test-topic not present in metadata after 6000 ms.";
        test:assertEquals(result.message(), errorMsg);
    } else {
        test:assertFail(msg = "Expected an error");
    }
}

@test:Config {enable: true}
function producerAdditionalPropertiesTest() returns error? {
    string topic = "producer-additional-properties-test-topic";
    kafkaTopics.push(topic);
    map<string> propertyMap = {
        "security.protocol": PROTOCOL_SASL_PLAINTEXT
    };

    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-08",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        auth: authConfig,
        additionalProperties: propertyMap
    };

    Producer kafkaProducer = check new (SASL_URL, producerConfigs);
    Error? result = kafkaProducer->send({topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    check kafkaProducer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "sasl-producer-test-group",
        clientId: "test-consumer-50"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function sslProducerTest() returns error? {
    string topic = "ssl-producer-test-topic";
    kafkaTopics.push(topic);

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-09",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer producer = check new (SSL_URL, producerConfiguration);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-producer-test-group",
        clientId: "test-consumer-51"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function sslCertKeyProducerTest() returns error? {
    string topic = "ssl-cert-key-producer-test-topic";
    kafkaTopics.push(topic);

    CertKey certKey = {
        certFile: SSL_CLIENT_PUBLIC_CERT_FILE_PATH,
        keyFile: SSL_CLIENT_PRIVATE_KEY_FILE_PATH
    };

    SecureSocket certSocket = {
        cert: SSL_BROKER_PUBLIC_CERT_FILE_PATH,
        key: certKey,
        protocol: {
            name: SSL
        }
    };

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-10",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: certSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer producer = check new (SSL_URL, producerConfiguration);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-cert-key-producer-test-group",
        clientId: "test-consumer-52"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function sslCertOnlyProducerTest() returns error? {
    string topic = "ssl-cert-only-producer-test-topic";
    kafkaTopics.push(topic);

    SecureSocket certSocket = {
        cert: SSL_BROKER_PUBLIC_CERT_FILE_PATH,
        protocol: {
            name: SSL
        }
    };

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-11",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: certSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer producer = check new (SSL_URL, producerConfiguration);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-cert-only-producer-test-group",
        clientId: "test-consumer-53"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function SSLWithSASLAuthProducerTest() returns error? {
    string topic = "ssl-with-sasl-auth-producer-test-topic";
    kafkaTopics.push(topic);

    SecureSocket certSocket = {
        cert: SSL_BROKER_PUBLIC_CERT_FILE_PATH,
        protocol: {
            name: SSL
        }
    };
    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-12",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: certSocket,
        securityProtocol: PROTOCOL_SASL_SSL
    };

    Producer|Error res = new (SASL_SSL_URL, producerConfigs);
    test:assertTrue(res is Error);
    if res is Error {
        test:assertEquals(res.message(), "Failed to initialize the producer: Could not find a 'KafkaClient' entry in the JAAS configuration. System property 'java.security.auth.login.config' is not set");
    }
    producerConfigs = {
        clientId: "test-producer-13",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: socket,
        securityProtocol: PROTOCOL_SSL
    };

    Producer producer = check new (SASL_SSL_URL, producerConfigs);
    Error? result = producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to send data to Kafka server: Topic ssl-with-sasl-auth-producer-test-topic not present in metadata after 6000 ms.");
    }
    check producer->close();

    producerConfigs = {
        clientId: "test-producer-14",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        auth: authConfig,
        secureSocket: socket,
        securityProtocol: PROTOCOL_SASL_SSL
    };

    producer = check new (SASL_SSL_URL, producerConfigs);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->close();

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "ssl-cert-only-producer-test-group",
        clientId: "test-consumer-53"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1, "Expected: 1. Received: " + consumerRecords.length().toString());
    check consumer->close();
}

@test:Config {enable: true}
function SASLOnSSLEndpointProducerTest() returns error? {
    string topic = "sasl-on-ssl-endpoint-producer-test-topic";

    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-15",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        auth: authConfig,
        securityProtocol: PROTOCOL_SASL_PLAINTEXT
    };

    Producer producer = check new (SSL_URL, producerConfigs);
    Error? result = producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to send data to Kafka server: Topic sasl-on-ssl-endpoint-producer-test-topic not present in metadata after 6000 ms.");
    }
    check producer->close();
}

@test:Config {enable: true}
function operationsOnClosedProducerTest() returns error? {
    string topic = "operations-on-closed-producer";
    kafkaTopics.push(topic);
    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-16"
    };
    Producer producer = check new (DEFAULT_URL, producerConfiguration);
    check producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    check producer->close();

    TopicPartition[]|Error? result = producer->getTopicPartitions("ssl-cert-only-producer-test-topic");
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to fetch partitions from the producer Requested metadata update after close");
    }
}

@test:Config {enable: true}
function producerAuthWithoutSecurityConfigsTest() returns error? {
    string topic = "producer-auth-without-security-configs-test-topic";

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-17",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        securityProtocol: PROTOCOL_PLAINTEXT
    };
    Producer producer = check new (SSL_URL, producerConfiguration);
    Error? result = producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to send data to Kafka server: Topic producer-auth-without-security-configs-test-topic not present in metadata after 6000 ms.");
    }

    producer = check new (SASL_URL, producerConfiguration);
    result = producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to send data to Kafka server: Topic producer-auth-without-security-configs-test-topic not present in metadata after 6000 ms.");
    }
}

@test:Config {enable: true}
function sslIncorrectStoresTest() returns error? {
    string topic = "ssl-incorrect-stores-test-topic";
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

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-17",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer producer = check new (SSL_URL, producerConfiguration);
    Error? result = producer->send({ topic: topic, value: TEST_MESSAGE.toBytes() });
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to send data to Kafka server: SSL handshake failed");
    }
}

@test:Config {enable: true}
function sslIncorrectMasterPasswordTest() returns error? {
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

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-18",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer|Error result = new (SSL_URL, producerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to initialize the producer: Failed to load SSL keystore tests/secrets/trustoresandkeystores/kafka.client.keystore.jks of type JKS");
    }
}

@test:Config {enable: true}
function sslIncorrectCertPathTest() returns error? {
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

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-19",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        secureSocket: invalidSocket,
        securityProtocol: PROTOCOL_SSL
    };
    Producer|Error result = new (SSL_URL, producerConfiguration);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(result.message(), "Failed to initialize the producer: Failed to load SSL keystore tests/secrets/trustoresa#ndkeystores/kafka.client.keystore.jks of type JKS");
    }
}
