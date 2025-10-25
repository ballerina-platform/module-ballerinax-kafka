// Copyright (c) 2025 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import ballerina/test;

// Test constants for error scenarios
const string INVALID_BROKER_URL = "invalid.broker.host:9092";
const string UNREACHABLE_BROKER_URL = "localhost:9999";
const string SASL_TEST_URL = "localhost:9093";
const string SSL_TEST_URL = "localhost:9094";

const string VALID_SASL_USER = "admin";
const string VALID_SASL_PASSWORD = "password";
const string INVALID_SASL_USER = "invalid_user";
const string INVALID_SASL_PASSWORD = "wrong_password";

const string VALID_SSL_TRUSTSTORE = "tests/resources/secrets/trustoresandkeystores/kafka.client.truststore.jks";
const string VALID_SSL_KEYSTORE = "tests/resources/secrets/trustoresandkeystores/kafka.client.keystore.jks";
const string INVALID_SSL_TRUSTSTORE = "tests/resources/invalid/path/truststore.jks";
const string SSL_PASSWORD = "password";
const string INVALID_SSL_PASSWORD = "wrong_password";

const decimal SHORT_TIMEOUT = 2;
const string TEST_TOPIC = "connection-error-test-topic";
const string TEST_GROUP = "connection-error-test-group";

// Test: Consumer connection timeout error
@test:Config {}
function testConsumerConnectionTimeout() returns error? {
    Consumer|Error consumerResult = new (UNREACHABLE_BROKER_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC]
    });

    if consumerResult is Error {
        test:assertFail("Consumer initialization should succeed (lazy connection)");
    }

    Consumer consumer = consumerResult;

    // Error should occur during first poll
    ConsumerRecord[]|Error pollResult = consumer->poll(SHORT_TIMEOUT);

    if pollResult is Error {
        string errorMsg = pollResult.message();
        test:assertTrue(
            errorMsg.includes("timeout") || errorMsg.includes("Timeout"),
            "Error message should mention timeout. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("Bootstrap servers") || errorMsg.includes("brokers"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected timeout error during poll, but succeeded");
    }

    check consumer->close();
}

// Test: Consumer with invalid hostname
@test:Config {}
function testConsumerInvalidHostname() returns error? {
    Consumer|Error consumerResult = new (INVALID_BROKER_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC]
    });

    if consumerResult is Error {
        test:assertFail("Consumer initialization should succeed (lazy connection)");
    }

    Consumer consumer = consumerResult;

    // Error should occur during first poll
    ConsumerRecord[]|Error pollResult = consumer->poll(SHORT_TIMEOUT);

    if pollResult is Error {
        string errorMsg = pollResult.message();
        test:assertTrue(
            errorMsg.includes("UnknownHost") || errorMsg.includes("hostname") || errorMsg.includes("resolve"),
            "Error message should mention hostname resolution issue. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("DNS") || errorMsg.includes("hostnames"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected hostname resolution error during poll, but succeeded");
    }

    check consumer->close();
}

// Test: Consumer SASL authentication failure
@test:Config {}
function testConsumerSaslAuthenticationFailure() returns error? {
    AuthenticationConfiguration invalidAuth = {
        mechanism: AUTH_SASL_PLAIN,
        username: INVALID_SASL_USER,
        password: INVALID_SASL_PASSWORD
    };

    Consumer|Error consumerResult = new (SASL_TEST_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC],
        auth: invalidAuth
    });

    if consumerResult is Error {
        test:assertFail("Consumer initialization should succeed (lazy connection)");
    }

    Consumer consumer = consumerResult;

    // Error should occur during first poll
    ConsumerRecord[]|Error pollResult = consumer->poll(SHORT_TIMEOUT);

    if pollResult is Error {
        string errorMsg = pollResult.message();
        test:assertTrue(
            errorMsg.includes("SASL") || errorMsg.includes("Authentication") || errorMsg.includes("authentication"),
            "Error message should mention SASL authentication issue. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("username") || errorMsg.includes("password") || errorMsg.includes("credentials"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected SASL authentication error during poll, but succeeded");
    }

    check consumer->close();
}

// Test: Consumer SSL certificate error
@test:Config {}
function testConsumerSslCertificateError() returns error? {
    crypto:TrustStore invalidTrustStore = {
        path: INVALID_SSL_TRUSTSTORE,
        password: SSL_PASSWORD
    };

    SecureSocket invalidSocket = {
        cert: invalidTrustStore
    };

    Consumer|Error consumerResult = new (SSL_TEST_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC],
        secureSocket: invalidSocket
    });

    if consumerResult is Error {
        test:assertFail("Consumer initialization should succeed (lazy connection)");
    }

    Consumer consumer = consumerResult;

    // Error should occur during first poll
    ConsumerRecord[]|Error pollResult = consumer->poll(SHORT_TIMEOUT);

    if pollResult is Error {
        string errorMsg = pollResult.message();
        test:assertTrue(
            errorMsg.includes("SSL") || errorMsg.includes("certificate") || errorMsg.includes("truststore"),
            "Error message should mention SSL/certificate issue. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("Certificate paths") || errorMsg.includes("Truststore") || errorMsg.includes("valid"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected SSL certificate error during poll, but succeeded");
    }

    check consumer->close();
}

// Test: Producer connection timeout error
@test:Config {}
function testProducerConnectionTimeout() returns error? {
    Producer|Error producerResult = new (UNREACHABLE_BROKER_URL);

    if producerResult is Error {
        test:assertFail("Producer initialization should succeed (lazy connection)");
    }

    Producer producer = producerResult;

    // Error should occur during first send
    Error? sendResult = producer->send({
        topic: TEST_TOPIC,
        value: "test message".toBytes()
    });

    if sendResult is Error {
        string errorMsg = sendResult.message();
        test:assertTrue(
            errorMsg.includes("timeout") || errorMsg.includes("Timeout"),
            "Error message should mention timeout. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("Bootstrap servers") || errorMsg.includes("brokers") || errorMsg.includes("accessible"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected timeout error during send, but succeeded");
    }

    check producer->close();
}

// Test: Producer SASL authentication failure
@test:Config {}
function testProducerSaslAuthenticationFailure() returns error? {
    AuthenticationConfiguration invalidAuth = {
        mechanism: AUTH_SASL_PLAIN,
        username: INVALID_SASL_USER,
        password: INVALID_SASL_PASSWORD
    };

    Producer|Error producerResult = new (SASL_TEST_URL, {
        auth: invalidAuth
    });

    if producerResult is Error {
        test:assertFail("Producer initialization should succeed (lazy connection)");
    }

    Producer producer = producerResult;

    // Error should occur during first send
    Error? sendResult = producer->send({
        topic: TEST_TOPIC,
        value: "test message".toBytes()
    });

    if sendResult is Error {
        string errorMsg = sendResult.message();
        test:assertTrue(
            errorMsg.includes("SASL") || errorMsg.includes("Authentication") || errorMsg.includes("authentication"),
            "Error message should mention SASL authentication issue. Got: " + errorMsg
        );
        test:assertTrue(
            errorMsg.includes("username") || errorMsg.includes("password") || errorMsg.includes("mechanism"),
            "Error message should provide troubleshooting guidance. Got: " + errorMsg
        );
    } else {
        test:assertFail("Expected SASL authentication error during send, but succeeded");
    }

    check producer->close();
}

// Test: Listener with connection error during start
@test:Config {}
function testListenerConnectionErrorOnStart() returns error? {
    Listener|Error listenerResult = new (UNREACHABLE_BROKER_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC]
    });

    if listenerResult is Error {
        test:assertFail("Listener initialization should succeed (lazy connection)");
    }

    Listener kafkaListener = listenerResult;

    // Attach a service
    error? attachResult = kafkaListener.attach(testService);
    if attachResult is error {
        test:assertFail("Service attach should succeed. Got error: " + attachResult.message());
    }

    // Error should occur during start when polling begins
    error? startResult = kafkaListener.'start();

    // Note: The listener's start() might succeed initially, but errors will be logged
    // when the background polling thread tries to connect. For this test, we verify
    // that the listener doesn't crash and can be stopped gracefully.

    if startResult is error {
        string errorMsg = startResult.message();
        test:assertTrue(
            errorMsg.includes("timeout") || errorMsg.includes("connect") || errorMsg.includes("Connection"),
            "Error message should indicate connection issue. Got: " + errorMsg
        );
    }

    // Clean up
    check kafkaListener.gracefulStop();
}

// Test: Listener with SASL authentication error
@test:Config {}
function testListenerSaslAuthenticationError() returns error? {
    AuthenticationConfiguration invalidAuth = {
        mechanism: AUTH_SASL_PLAIN,
        username: INVALID_SASL_USER,
        password: INVALID_SASL_PASSWORD
    };

    Listener|Error listenerResult = new (SASL_TEST_URL, {
        groupId: TEST_GROUP,
        topics: [TEST_TOPIC],
        auth: invalidAuth
    });

    if listenerResult is Error {
        test:assertFail("Listener initialization should succeed (lazy connection)");
    }

    Listener kafkaListener = listenerResult;

    // Attach a service
    error? attachResult = kafkaListener.attach(testService);
    if attachResult is error {
        test:assertFail("Service attach should succeed. Got error: " + attachResult.message());
    }

    // Error should occur during start when polling begins
    error? startResult = kafkaListener.'start();

    // Note: Similar to above, errors will be logged in background thread
    if startResult is error {
        string errorMsg = startResult.message();
        test:assertTrue(
            errorMsg.includes("SASL") || errorMsg.includes("Authentication") || errorMsg.includes("authentication"),
            "Error message should indicate SASL authentication issue. Got: " + errorMsg
        );
    }

    // Clean up
    check kafkaListener.gracefulStop();
}

// Test service for listener tests
Service testService = service object {
    remote function onConsumerRecord(Caller caller, ConsumerRecord[] records) returns error? {
        // No-op service for testing
    }
};
