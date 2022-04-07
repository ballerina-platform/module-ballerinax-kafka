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
    string[] records = check consumer->pollWithType(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(string value) {
        test:assertEquals(value, TEST_MESSAGE);
    });
    check consumer->close();
}

@test:Config {enable: true}
function intBindingConsumerTest() returns error? {
    string topic = "int-binding-consumer-test-topic";
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
    int[] records = check consumer->pollWithType(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(int value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function floatBindingConsumerTest() returns error? {
    string topic = "float-binding-consumer-test-topic";
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
    float[] records = check consumer->pollWithType(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(float value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function decimalBindingConsumerTest() returns error? {
    string topic = "decimal-binding-consumer-test-topic";
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
    decimal[] records = check consumer->pollWithType(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(decimal value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function booleanBindingConsumerTest() returns error? {
    string topic = "boolean-binding-consumer-test-topic";
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
    boolean[] records = check consumer->pollWithType(2);
    test:assertEquals(records.length(), 3);
    records.forEach(function(boolean value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function xmlBindingConsumerTest() returns error? {
    string topic = "xml-binding-consumer-test-topic";
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
    xml[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(xml value) {
        test:assertEquals(value, sendingValue);
    });
    check consumer->close();
}

@test:Config {enable: true}
function jsonBindingConsumerTest() returns error? {
    string topic = "json-binding-consumer-test-topic";
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
    json[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(json value) {
        test:assertEquals(value, jsonData);
    });
    check consumer->close();
}

@test:Config {enable: true}
function mapBindingConsumerTest() returns error? {
    string topic = "map-binding-consumer-test-topic";
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
    map<Person>[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(map<Person> value) {
        test:assertEquals(value, personMap);
    });
    check consumer->close();
}

@test:Config {enable: true}
function tableBindingConsumerTest() returns error? {
    string topic = "table-binding-consumer-test-topic";
    table<Person> key(name) personMapTable = table [];
    personMapTable.add(personRecord1);
    personMapTable.add(personRecord2);
    personMapTable.add(personRecord3);

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
    table<Person>[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(table<Person> value) {
        test:assertEquals(value.toString(), personMapTable.toString());
    });
    check consumer->close();
}

@test:Config {enable: true}
function recordBindingConsumerTest() returns error? {
    string topic = "record-binding-consumer-test-topic";
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
    Person[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    records.forEach(function(Person value) {
        test:assertEquals(value, personRecord1);
    });
    check consumer->close();
}

@test:Config {enable: true}
function nilBindingConsumerTest() returns error? {
    string topic = "nil-binding-consumer-test-topic";
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
    ()[] records = check consumer->pollWithType(5);
    test:assertEquals(records.length(), 3);
    check consumer->close();
}

@test:Config {enable: true}
function dataBindingErrorConsumerTest() returns error? {
    string topic = "data-binding-error-consumer-test-topic";
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);
    check sendMessage(personRecord1.toString().toBytes(), topic);

    ConsumerConfiguration consumerConfigs = {
        topics: [topic],
        groupId: "data-binding-consumer-group-12",
        clientId: "data-binding-consumer-id-12",
        offsetReset: OFFSET_RESET_EARLIEST
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfigs);
    int[]|Error result = consumer->pollWithType(5);
    test:assertTrue(result is Error);
    if result is Error {
        log:printInfo(result.message());
        test:assertTrue(result.message().startsWith("Data binding failed"));
    }
    check consumer->close();
}
