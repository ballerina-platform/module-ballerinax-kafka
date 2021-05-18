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
import ballerina/test;
import ballerina/io;

string MESSAGE_KEY = "TEST-KEY";

@test:Config{}
function producerInitTest() returns error? {
    ProducerConfiguration producerConfiguration1 = {
        clientId: "test-producer-01",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3
    };
    ProducerConfiguration producerConfiguration2 = {
        clientId: "test-producer-02",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        transactionalId: "prod-id-1"
    };
    ProducerConfiguration producerConfiguration3 = {
        clientId: "test-producer-03",
        acks: ACKS_ALL,
        maxBlock: 6,
        requestTimeout: 2,
        retryCount: 3,
        transactionalId: "prod-id-2",
        enableIdempotence: true
    };
    (Producer|Error|error)? result1 = check new (DEFAULT_URL, producerConfiguration1);
    (Producer|Error|error)? result2 = trap new (DEFAULT_URL, producerConfiguration2);
    (Producer|Error|error)? result3 = check new (DEFAULT_URL, producerConfiguration3);
    if (result1 is Error || result1 is error) {
        test:assertFail(msg = result1.message());
    }
    if (result2 is Error || result2 is error) {
        string expectedErr = "configuration enableIdempotence must be set to true to enable " +
            "transactional producer";
         test:assertEquals(result2.message(), expectedErr);
    } else {
        test:assertFail(msg = "Expected an error");
    }
    if (result3 is Error || result3 is error) {
        test:assertFail(msg = result3.message());
    }
}

@test:Config {}
function producerSendStringTest() returns error? {
    Producer stringProducer = check new (DEFAULT_URL, producerConfiguration);
    string message = "Hello, Ballerina";
    Error? result = stringProducer->send({ topic: topic3, value: message.toBytes() });
    test:assertFalse(result is error, result is error ? result.toString() : result.toString());
    result = stringProducer->send({ topic: topic3, value: message.toBytes(), key: MESSAGE_KEY.toBytes() });

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic3],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-functions-test-group",
        clientId: "test-producer-04"
    };
    Consumer stringConsumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check stringConsumer->poll(3);
    test:assertEquals(consumerRecords.length(), 2);
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

@test:Config {}
function producerFlushTest() returns error? {
    Producer flushTestProducer = check new (DEFAULT_URL, producerConfiguration);
    check flushTestProducer->send({ topic: topic1, value: TEST_MESSAGE.toBytes() });
    check flushTestProducer->'flush();
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic1],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "producer-flush-test-group",
        clientId: "test-producer-05"
    };
    Consumer stringConsumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check stringConsumer->poll(3);
    test:assertEquals('string:fromBytes(consumerRecords[0].value), TEST_MESSAGE);
}

@test:Config {}
function producerGetTopicPartitionsTest() returns error? {
    Producer topicPartitionTestProducer = check new (DEFAULT_URL, producerConfiguration);
    TopicPartition[]|Error topicPartitions = check topicPartitionTestProducer->getTopicPartitions(topic1);
    if (topicPartitions is error) {
        test:assertFail(msg = "Invalid result received");
    } else {
        test:assertEquals(topicPartitions[0].partition, 0, "Expected: 0. Received: " + topicPartitions[0].partition.toString());
    }
    check topicPartitionTestProducer->close();
}

@test:Config {}
function producerTransactionalProducerTest() returns error? {
    ProducerConfiguration producerConfigs = {
        clientId: "test-producer-06",
        acks: "all",
        retryCount: 3,
        enableIdempotence: true,
        transactionalId: "test-transactional-id"
    };
    Producer transactionalProducer = check new (DEFAULT_URL, producerConfigs);
    transaction {
        check transactionalProducer->send({
            topic: topic3,
            value: TEST_MESSAGE.toBytes(),
            partition: 0
        });
        var commitResult = commit;
        if (commitResult is ()) {
            io:println("Commit successful");
        } else {
            test:assertFail(msg = "Commit Failed");
        }
    }
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic3],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-transactional-test-group",
        clientId: "test-consumer-38"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 4, "Expected: 4. Received: " + consumerRecords.length().toString());
}
