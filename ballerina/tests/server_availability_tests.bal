// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
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

import ballerina/lang.runtime as runtime;
import ballerina/test;

string serverDownErrorMessage = "";
boolean onErrorCalled = false;

@test:Config {
    groups: ["listener", "server-availability"]
}
function testListenerWithServerDown() returns error? {
    string topic = "server-down-listener-test-topic";
    // Note: Don't push to kafkaTopics since this topic won't be created (INCORRECT_KAFKA_URL)

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "server-down-listener-test-group",
        clientId: "test-listener-server-down",
        pollingInterval: 1000
    };

    Listener consumer = check new (INCORRECT_KAFKA_URL, consumerConfiguration);
    check consumer.attach(serverDownListenerService);
    check consumer.'start();

    runtime:sleep(5);

    test:assertTrue(onErrorCalled);
    string expectedError = "Server might not be available at " + INCORRECT_KAFKA_URL + ". No active connections found.";
    test:assertEquals(serverDownErrorMessage, expectedError);

    check consumer.gracefulStop();

    serverDownErrorMessage = "";
    onErrorCalled = false;
}

@test:Config {
    groups: ["consumer", "server-availability"]
}
function testConsumerClientWithServerDown() returns error? {
    string topic = "server-down-consumer-test-topic";
    // Note: Don't push to kafkaTopics since this topic won't be created (INCORRECT_KAFKA_URL)

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "server-down-consumer-test-group",
        clientId: "test-consumer-server-down"
    };

    Consumer consumer = check new (INCORRECT_KAFKA_URL, consumerConfiguration);
    BytesConsumerRecord[]|error result = consumer->poll(3);
    test:assertTrue(result is error);
    if result is error {
        string expectedError = "Server might not be available at " + INCORRECT_KAFKA_URL + ". No active connections found.";
        test:assertEquals(result.message(), expectedError);
    }
    check consumer->close();
}

@test:Config {
    groups: ["consumer", "server-availability"]
}
function testConsumerPollPayloadWithServerDown() returns error? {
    string topic = "server-down-poll-payload-test-topic";
    // Note: Don't push to kafkaTopics since this topic won't be created (INCORRECT_KAFKA_URL)

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "server-down-poll-payload-test-group",
        clientId: "test-consumer-poll-payload-server-down"
    };

    Consumer consumer = check new (INCORRECT_KAFKA_URL, consumerConfiguration);
    string[]|error result = consumer->pollPayload(3);
    test:assertTrue(result is error);
    if result is error {
        string expectedError = "Server might not be available at " + INCORRECT_KAFKA_URL + ". No active connections found.";
        test:assertEquals(result.message(), expectedError);
    }
    check consumer->close();
}

@test:Config {
    groups: ["listener", "server-availability"]
}
function testListenerWithServerGoingDown() returns error? {
    string topic = "server-going-down-test-topic";
    kafkaTopics.push(topic);

    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "server-going-down-test-group",
        clientId: "test-listener-going-down",
        pollingInterval: 1000
    };

    Listener consumer = check new (DEFAULT_URL, consumerConfiguration);
    check consumer.attach(serverGoingDownListenerService);
    check consumer.'start();

    runtime:sleep(2);
    check consumer.gracefulStop();
}

Service serverDownListenerService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records) {
    }

    remote function onError(Error 'error) {
        lock {
            onErrorCalled = true;
            serverDownErrorMessage = 'error.message();
        }
    }
};

@test:Config {
    groups: ["producer", "server-availability"]
}
function testProducerSendWithServerDown() returns error? {
    string topic = "server-down-producer-send-test-topic";
    // Note: Don't push to kafkaTopics since this topic won't be created (INCORRECT_KAFKA_URL)

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-server-down"
    };

    Producer producer = check new (INCORRECT_KAFKA_URL, producerConfiguration);
    Error? result = producer->send({topic: topic, value: "test-message".toBytes()});
    test:assertTrue(result is error);
    if result is error {
        string expectedError = "Server might not be available at " + INCORRECT_KAFKA_URL + ". No active connections found.";
        test:assertEquals(result.message(), expectedError);
    }
    check producer->close();
}

@test:Config {
    groups: ["producer", "server-availability"]
}
function testProducerSendWithMetadataWithServerDown() returns error? {
    string topic = "server-down-producer-send-metadata-test-topic";
    // Note: Don't push to kafkaTopics since this topic won't be created (INCORRECT_KAFKA_URL)

    ProducerConfiguration producerConfiguration = {
        clientId: "test-producer-send-metadata-server-down"
    };

    Producer producer = check new (INCORRECT_KAFKA_URL, producerConfiguration);
    RecordMetadata|Error result = producer->sendWithMetadata({topic: topic, value: "test-message".toBytes()});
    test:assertTrue(result is error);
    if result is error {
        string expectedError = "Server might not be available at " + INCORRECT_KAFKA_URL + ". No active connections found.";
        test:assertEquals(result.message(), expectedError);
    }
    check producer->close();
}

// Service for testing when server goes down after startup
Service serverGoingDownListenerService = service object {
    remote function onConsumerRecord(BytesConsumerRecord[] records) {
    }

    remote function onError(Error 'error) {
        // This service handles errors when server goes down
    }
};
