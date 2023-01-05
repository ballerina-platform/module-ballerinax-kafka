// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/test;
import ballerina/log;

@test:Config {enable: true}
function stringBindingConsumerTest() returns error? {
    string topic = "string-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-01",
        clientId: "data-binding-consumer-id-01",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    string[] records = check consumer->pollPayload(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(string value) {
        test:assertEquals(value, TEST_MESSAGE);
    });
    check consumer->close();
}

@test:Config {enable: true}
function intBindingConsumerTest() returns error? {
    string topic = "int-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    int sendingValue = 100;
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-02",
        clientId: "data-binding-consumer-id-02",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    int[] records = check consumer->pollPayload(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(int value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function floatBindingConsumerTest() returns error? {
    string topic = "float-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    float sendingValue = 100.9;
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-03",
        clientId: "data-binding-consumer-id-03",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    float[] records = check consumer->pollPayload(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(float value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function decimalBindingConsumerTest() returns error? {
    string topic = "decimal-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    decimal sendingValue = 10.4d;
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-04",
        clientId: "data-binding-consumer-id-04",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    decimal[] records = check consumer->pollPayload(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(decimal value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function booleanBindingConsumerTest() returns error? {
    string topic = "boolean-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    boolean sendingValue = true;
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-05",
        clientId: "data-binding-consumer-id-05",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    boolean[] records = check consumer->pollPayload(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(boolean value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function xmlBindingConsumerTest() returns error? {
    string topic = "xml-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    xml sendingValue = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(sendingValue, topic);
    check sendMessage(sendingValue, topic);
    check sendMessage(sendingValue, topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-06",
        clientId: "data-binding-consumer-id-06",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    xml[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(xml value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function jsonBindingConsumerTest() returns error? {
    string topic = "json-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(jsonData.toString().toBytes(), topic);
    check sendMessage(jsonData.toString().toBytes(), topic);
    check sendMessage(jsonData.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-07",
        clientId: "data-binding-consumer-id-07",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    json[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(json value) {
        test:assertEquals(value, jsonData);
    });
    check consumer->close();
}

@test:Config {enable: true}
function mapBindingConsumerTest() returns error? {
    string topic = "map-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personMap.toString().toBytes(), topic);
    check sendMessage(personMap.toString().toBytes(), topic);
    check sendMessage(personMap.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-08",
        clientId: "data-binding-consumer-id-08",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    map<Person>[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(map<Person> value) {
        test:assertEquals(value, personMap);
    });
    check consumer->close();
}

@test:Config {enable: true}
function tableBindingConsumerTest() returns error? {
    string topic = "table-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    table<Person> personMapTable = table [];

    personMapTable.add(personRecord1);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-09",
        clientId: "data-binding-consumer-id-09",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    table<Person>[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(table<Person> value) {
        test:assertEquals(value, personMapTable);
    });
    check consumer->close();
}

@test:Config {enable: true}
function recordBindingConsumerTest() returns error? {
    string topic = "record-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    Person[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(Person value) {
        test:assertEquals(value, personRecord1);
    });
    check consumer->close();
}

@test:Config {enable: true}
function nilBindingConsumerTest() returns error? {
    string topic = "nil-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage((), topic);
    check sendMessage((), topic);
    check sendMessage((), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-11",
        clientId: "data-binding-consumer-id-11",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    ()[] records = check consumer->pollPayload(5);
    test:assertEquals(records.length(), 3);
    check consumer->close();
}

@test:Config {enable: true}
function dataBindingErrorConsumerTest() returns error? {
    string topic = "data-binding-error-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-12",
        clientId: "data-binding-consumer-id-12",
        offsetReset: OFFSET_RESET_EARLIEST,
        autoSeekOnValidationFailure: false
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    xml[]|Error result = consumer->pollPayload(5);
    test:assertTrue(result is Error);
    if result is Error {
        log:printInfo(result.message());
        test:assertTrue(result.message().startsWith("Data binding failed"));
    }
    check consumer->close();
}

@test:Config {enable: true}
function stringConsumerRecordTest() returns error? {
    string topic = "string-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    StringConsumerRecord[] records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(StringConsumerRecord value) {
        test:assertEquals(value.value, TEST_MESSAGE);
    });
    check consumer->close();
}

@test:Config {enable: true}
function intConsumerRecordTest() returns error? {
    string topic = "int-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(1.toString().toBytes(), topic);
    check sendMessage(1.toString().toBytes(), topic);
    check sendMessage(1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    IntConsumerRecord[] records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(IntConsumerRecord value) {
        test:assertEquals(value.value, 1);
    });
    check consumer->close();
}

@test:Config {enable: true}
function xmlConsumerRecordTest() returns error? {
    string topic = "xml-consumer-record-test-topic";
    kafkaTopics.push(topic);
    xml sendingValue = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);
    check sendMessage(sendingValue.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    XmlConsumerRecord[] records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(XmlConsumerRecord value) {
        test:assertEquals(value.value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function recordConsumerRecordTest() returns error? {
    string topic = "record-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    PersonConsumerRecord[] records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(PersonConsumerRecord value) {
        test:assertEquals(value.value, personRecord1);
    });
    check consumer->close();
}

@test:Config {enable: true}
function jsonConsumerRecordTest() returns error? {
    string topic = "json-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(jsonData.toString().toBytes(), topic);
    check sendMessage(jsonData.toString().toBytes(), topic);
    check sendMessage(jsonData.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    JsonConsumerRecord[] records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(JsonConsumerRecord value) {
        test:assertEquals(value.value, jsonData);
    });
    check consumer->close();
}

@test:Config {enable: true}
function readonlyConsumerRecordTest() returns error? {
    string topic = "readonly-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    PersonConsumerRecord[] & readonly records = check consumer->poll(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(PersonConsumerRecord value) {
        test:assertTrue(value.isReadOnly());
    });
    check consumer->close();
}

@test:Config {enable: true}
function unionBindingConsumerTest() returns error? {
    string topic = "union-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);
    check sendMessage(TEST_MESSAGE.toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-08",
        clientId: "data-binding-consumer-id-08",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    (int|string)[] records = check consumer->pollPayload(1);
    test:assertEquals(records.length(), 3);
    records.forEach(function(int|string value) {
        test:assertEquals(value, TEST_MESSAGE);
    });
    check consumer->close();
}

@test:Config {enable: true}
function readonlyRecordBindingConsumerTest() returns error? {
    string topic = "readonly-record-binding-consumer-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-09",
        clientId: "data-binding-consumer-id-09",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    readonly & Person[] records = check consumer->pollPayload(5);
    test:assertTrue(records.isReadOnly());
    test:assertEquals(records.length(), 3);
    records.forEach(function(Person value) {
        test:assertEquals(value, personRecord1);
    });
    check consumer->close();
}

@test:Config {enable: true}
function pollErrorWithSeekConsumerRecordTest() returns error? {
    string topic = "poll-error-with-seek-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1, topic);
    check sendMessage("Invalid", topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST,
        autoSeekOnValidationFailure: false
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);

    PersonConsumerRecord[] value = check consumer->poll(5);
    test:assertEquals(value.length(), 1);
    test:assertEquals(value[0].value, personRecord1);
    PersonConsumerRecord[]|error result = consumer->poll(5);
    if result is PayloadBindingError {
        test:assertEquals(result.message(), "Data binding failed. If needed, please seek past the record to continue consumption.");
        check consumer->seek({
            partition: result.detail().partition,
            offset: result.detail().offset + 1
        });
        result = consumer->poll(5);
        if result is error {
            test:assertFail(result.message());
        } else {
            test:assertEquals(result.length(), 2);
            test:assertEquals(result[0].value, personRecord1);
            test:assertEquals(result[1].value, personRecord1);
        }
    } else {
        test:assertFail("Expected a payload binding error");
    }
    check consumer->close();
}

@test:Config {enable: true}
function recordCastingErrorPollTest() returns error? {
    string topic = "record-casting-error-poll-test-topic";
    kafkaTopics.push(topic);
    check sendMessage({
        name: "ABC",
        age: 12,
        address: "test-address",
        married: false,
        id: 1231
    }.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-10",
        clientId: "data-binding-consumer-id-10",
        offsetReset: OFFSET_RESET_EARLIEST,
        autoSeekOnValidationFailure: false
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    PersonConsumerRecord[]|Error records = consumer->poll(5);
    if records is PayloadBindingError {
        test:assertEquals(records.message(), "Data binding failed. If needed, please seek past the record to continue consumption.");
    } else {
        test:assertFail("Expected a payload binding error");
    }
    check consumer->close();
}

@test:Config {enable: true}
function recordCastingErrorPollPayloadTest() returns error? {
    string topic = "record-casting-error-poll-payload-test-topic";
    kafkaTopics.push(topic);
    check sendMessage({
        name: "ABC",
        age: 12,
        address: "test-address",
        married: false,
        id: 1231
    }.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-11",
        clientId: "data-binding-consumer-id-11",
        offsetReset: OFFSET_RESET_EARLIEST,
        autoSeekOnValidationFailure: false
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    Person[]|Error records = consumer->pollPayload(5);
    if records is PayloadBindingError {
        test:assertEquals(records.message(), "Data binding failed. If needed, please seek past the record to continue consumption.");
    } else {
        test:assertFail("Expected a payload binding error");
    }
    check consumer->close();
}

@test:Config {enable: true}
function intCastingErrorPollPayloadWithAutoSeekTest() returns error? {
    string topic = "int-casting-error-poll-payload-with-auto-seek-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(12.toString().toBytes(), topic);
    check sendMessage("Invalid value".toBytes(), topic);
    check sendMessage(13.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-12",
        clientId: "data-binding-consumer-id-12",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    int[] values = check consumer->pollPayload(5);
    if values.length() != 2 {
        test:assertFail("Invalid record count");
    }
    test:assertEquals(values[0], 12);
    test:assertEquals(values[1], 13);
    check consumer->close();
}
