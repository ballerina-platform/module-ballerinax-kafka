// Copyright (c) 2024 WSO2 LLC. (http://www.wso2.com).
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

import ballerina/test;

type StringHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<string> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadStringHeaders() returns error? {
    string topic = "consumer-read-string-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-string-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    StringHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<string> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": "header1", "key2": "header3"});
    check consumer->close();
}

type StringArrayHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<string[]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadStringArrayHeaders() returns error? {
    string topic = "consumer-read-string-array-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-string-array-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    StringArrayHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<string[]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1", "header2"], "key2": ["header3"]});
    check consumer->close();
}

type ByteHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<byte[]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadByteHeaders() returns error? {
    string topic = "consumer-read-byte-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-byte-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ByteHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<byte[]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": "header1".toBytes(), "key2": "header3".toBytes()});
    check consumer->close();
}

type ByteArrayHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<byte[][]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadByteArrayHeaders() returns error? {
    string topic = "consumer-read-byte-array-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-byte-array-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ByteArrayHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<byte[][]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": ["header3".toBytes()]});
    check consumer->close();
}

type StringAndStringArrayHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<string|string[]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadStringAndStringArrayHeaders() returns error? {
    string topic = "consumer-read-string-string-array-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-string-string-array-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    StringAndStringArrayHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<string|string[]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1", "header2"], "key2": "header3"});
    check consumer->close();
}

type StringAndByteArrayHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<string|byte[]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadStringAndByteArrayHeaders() returns error? {
    string topic = "consumer-read-string-and-byte-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-string-and-byte-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    StringAndByteArrayHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<string|byte[]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": "header1".toBytes(), "key2": "header3"});
    check consumer->close();
}

type ByteAndStringArrayHeaderConsumerRecord record {|
    *AnydataConsumerRecord;
    map<string[]|byte[]> headers;
    string value;
|};

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadByteAndStringArrayHeaders() returns error? {
    string topic = "consumer-read-byte-and-string-array-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-byte-and-string-array-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ByteAndStringArrayHeaderConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<string[]|byte[]> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1", "header2"], "key2": "header3".toBytes()});
    check consumer->close();
}

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadByteAndByteArrayHeaders() returns error? {
    string topic = "consumer-read-byte-and-byte-array-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-byte-and-byte-array-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<byte[]|byte[][]|()?> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()});
    check consumer->close();
}

@test:Config {
    groups: ["consumer", "header"]
}
function testConsumerReadAllSupportedTypesHeaders() returns error? {
    string topic = "consumer-read-all-types-headers-test-topic";
    kafkaTopics.push(topic);
    map<byte[]|byte[][]|string|string[]>? headers = {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()};
    check sendMessage(TEST_MESSAGE.toBytes(), topic, (), headers);
    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "consumer-read-all-types-headers-test-group",
        clientId: "test-consumer-61"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    AnydataConsumerRecord[] consumerRecords = check consumer->poll(5);
    test:assertEquals(consumerRecords.length(), 1);
    map<byte[]|byte[][]|string|string[]|()> receivedHeaders = consumerRecords[0].headers;
    test:assertEquals(receivedHeaders, {"key1": ["header1".toBytes(), "header2".toBytes()], "key2": "header3".toBytes()});
    check consumer->close();
}
