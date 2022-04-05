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

import ballerina/lang.value;
import ballerina/test;

@test:Config {}
function intProduceTest() returns error? {
    string topic = "int-produce-test-topic";
    check producer->send({topic, value: 1});
    check producer->send({topic, value: 2});
    check producer->send({topic, value: 3});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-01",
        clientId: "data-binding-producer-01"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    int receivedValue = 0;
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue += checkpanic int:fromString(receivedMsg);
    });
    test:assertEquals(receivedValue, 6);
    check consumer->close();
}

@test:Config {}
function floatProduceTest() returns error? {
    string topic = "float-produce-test-topic";
    check producer->send({topic, value: 1.2});
    check producer->send({topic, value: 2.6});
    check producer->send({topic, value: 3.7});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-02",
        clientId: "data-binding-producer-02"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    float receivedValue = 0;
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue += checkpanic float:fromString(receivedMsg);
    });
    test:assertEquals(receivedValue, 7.5);
    check consumer->close();
}

@test:Config {}
function decimalProduceTest() returns error? {
    string topic = "decimal-produce-test-topic";
    check producer->send({topic, value: 1.7d});
    check producer->send({topic, value: 2.4d});
    check producer->send({topic, value: 0.9d});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-03",
        clientId: "data-binding-producer-03"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    decimal receivedValue = 0;
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue += checkpanic decimal:fromString(receivedMsg);
    });
    test:assertEquals(receivedValue, 5.0d);
    check consumer->close();
}

@test:Config {}
function booleanProduceTest() returns error? {
    string topic = "boolean-produce-test-topic";
    check producer->send({topic, value: true});
    check producer->send({topic, value: true});
    check producer->send({topic, value: true});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-04",
        clientId: "data-binding-producer-04"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string receivedValue = "false";
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        receivedValue = checkpanic 'string:fromBytes(cRecord.value);
    });
    test:assertEquals(receivedValue, "true");
    check consumer->close();
}

@test:Config {}
function stringProduceTest() returns error? {
    string topic = "string-produce-test-topic";
    check producer->send({topic, value: TEST_MESSAGE});
    check producer->send({topic, value: TEST_MESSAGE});
    check producer->send({topic, value: TEST_MESSAGE});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-05",
        clientId: "data-binding-producer-05"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string receivedValue = "";
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        receivedValue += checkpanic 'string:fromBytes(cRecord.value);
    });
    test:assertEquals(receivedValue, TEST_MESSAGE + TEST_MESSAGE + TEST_MESSAGE);
    check consumer->close();
}

@test:Config {}
function xmlProduceTest() returns error? {
    string topic = "xml-produce-test-topic";
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check producer->send({topic, value: xmlData});
    check producer->send({topic, value: xmlData});
    check producer->send({topic, value: xmlData});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-06",
        clientId: "data-binding-producer-06"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string[] receivedValues = [];
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        receivedValues.push(checkpanic 'string:fromBytes(cRecord.value));
    });
    test:assertEquals(receivedValues, [xmlData.toString(), xmlData.toString(), xmlData.toString()]);
    check consumer->close();
}

@test:Config {}
function recordProduceTest() returns error? {
    string topic = "record-produce-test-topic";
    check producer->send({topic, value: personRecord1});
    check producer->send({topic, value: personRecord1});
    check producer->send({topic, value: personRecord1});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-07",
        clientId: "data-binding-producer-07"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    Person[] receivedValues = [];
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        Person person = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(person);
    });
    test:assertEquals(receivedValues, [personRecord1, personRecord1, personRecord1]);
    check consumer->close();
}

@test:Config {}
function mapProduceTest() returns error? {
    string topic = "map-produce-test-topic";
    check producer->send({topic, value: personMap});
    check producer->send({topic, value: personMap});
    check producer->send({topic, value: personMap});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-08",
        clientId: "data-binding-producer-08"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    map<Person>[] receivedValues = [];
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        map<Person> personMap = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(personMap);
    });
    test:assertEquals(receivedValues, [personMap, personMap, personMap]);
    check consumer->close();
}

@test:Config {}
function tableProduceTest() returns error? {
    string topic = "table-produce-test-topic";
    table<Person> key(name) personMapTable = table [];
    personMapTable.add(personRecord1);
    personMapTable.add(personRecord2);
    personMapTable.add(personRecord3);
    check producer->send({topic, value: personMapTable});
    check producer->send({topic, value: personMapTable});
    check producer->send({topic, value: personMapTable});

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-09",
        clientId: "data-binding-producer-09"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    ConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    table<Person>[] receivedValues = [];
    consumerRecords.forEach(function(ConsumerRecord cRecord) {
        table<Person> personTable = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(personTable);
    });
    test:assertEquals(receivedValues.toString(), [personMapTable, personMapTable, personMapTable].toString());
    check consumer->close();
}
