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

public type IntProducerRecord record {|
    *AnydataProducerRecord;
    int key?;
    int value;
|};

public type FloatProducerRecord record {|
    *AnydataProducerRecord;
    float value;
    float key?;
|};

public type DecimalProducerRecord record {|
    *AnydataProducerRecord;
    decimal key?;
    decimal value;
|};

public type BooleanProducerRecord record {|
    *AnydataProducerRecord;
    boolean key?;
    boolean value;
|};

public type StringProducerRecord record {|
    *AnydataProducerRecord;
    string key?;
    string value;
|};

public type PersonProducerRecord record {|
    *AnydataProducerRecord;
    string key?;
    Person value;
|};

public type MapProducerRecord record {|
    *AnydataProducerRecord;
    map<Person> value;
    string key?;
|};

public type XmlProducerRecord record {|
    *AnydataProducerRecord;
    string key?;
    xml value;
|};

public type TableProducerRecord record {|
    *AnydataProducerRecord;
    string key?;
    table<Person> value;
|};

public type JsonProducerRecord record {|
    *AnydataProducerRecord;
    string key?;
    json value;
|};

@test:Config {
    groups: ["producer", "data-binding"]
}
function testIntProduce() returns error? {
    string topic = "int-produce-test-topic";
    kafkaTopics.push(topic);
    IntProducerRecord producerRecord = {
        topic,
        'key: 2,
        value: 10
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-01",
        clientId: "data-binding-producer-01"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    int receivedValue = 0;
    int receivedKey = 0;
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue += checkpanic int:fromString(receivedMsg);
        string receivedKeyString = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
        receivedKey += checkpanic int:fromString(receivedKeyString);
    });
    test:assertEquals(receivedValue, 30);
    test:assertEquals(receivedKey, 6);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testFloatProduce() returns error? {
    string topic = "float-produce-test-topic";
    kafkaTopics.push(topic);
    FloatProducerRecord producerRecord = {
        topic,
        'key: 100.9,
        value: 100.9
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-02",
        clientId: "data-binding-producer-02"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    float receivedValue = 0;
    float receivedKey = 0;
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue = checkpanic float:fromString(receivedMsg);
        string receivedKeyString = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
        receivedKey = checkpanic float:fromString(receivedKeyString);
    });
    test:assertEquals(receivedValue, 100.9);
    test:assertEquals(receivedKey, 100.9);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testDecimalProduce() returns error? {
    string topic = "decimal-produce-test-topic";
    kafkaTopics.push(topic);
    DecimalProducerRecord producerRecord = {
        topic,
        'key: 2.3d,
        value: 10.3d
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-03",
        clientId: "data-binding-producer-03"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    decimal receivedValue = 0;
    decimal receivedKey = 0;
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        string receivedMsg = checkpanic 'string:fromBytes(cRecord.value);
        receivedValue += checkpanic decimal:fromString(receivedMsg);
        string receivedKeyString = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
        receivedKey += checkpanic decimal:fromString(receivedKeyString);
    });
    test:assertEquals(receivedValue, 30.9d);
    test:assertEquals(receivedKey, 6.9d);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testBooleanProduce() returns error? {
    string topic = "boolean-produce-test-topic";
    kafkaTopics.push(topic);
    BooleanProducerRecord producerRecord = {
        topic,
        'key: true,
        value: true
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-04",
        clientId: "data-binding-producer-04"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string receivedValue = "false";
    string receivedKey = "";
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        receivedValue = checkpanic 'string:fromBytes(cRecord.value);
        receivedKey = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
    });
    test:assertEquals(receivedValue, "true");
    test:assertEquals(receivedKey, "true");
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testStringProduce() returns error? {
    string topic = "string-produce-test-topic";
    kafkaTopics.push(topic);
    StringProducerRecord producerRecord = {
        topic,
        'key: TEST_KEY,
        value: TEST_MESSAGE
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-05",
        clientId: "data-binding-producer-05"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string receivedValue = "";
    string receivedKey = "";
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        receivedValue = checkpanic 'string:fromBytes(cRecord.value);
        receivedKey = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
    });
    test:assertEquals(receivedValue, TEST_MESSAGE);
    test:assertEquals(receivedKey, TEST_KEY);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testXmlProduce() returns error? {
    string topic = "xml-produce-test-topic";
    kafkaTopics.push(topic);
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    XmlProducerRecord producerRecord = {
        topic,
        'key: TEST_KEY,
        value: xmlData
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-06",
        clientId: "data-binding-producer-06"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    string[] receivedValues = [];
    string receivedKey = "";
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        receivedValues.push(checkpanic 'string:fromBytes(cRecord.value));
        receivedKey = checkpanic 'string:fromBytes(<byte[]>cRecord?.'key);
    });
    test:assertEquals(receivedValues, [xmlData.toString(), xmlData.toString(), xmlData.toString()]);
    test:assertEquals(receivedKey, TEST_KEY);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testRecordProduce() returns error? {
    string topic = "record-produce-test-topic";
    kafkaTopics.push(topic);
    PersonProducerRecord producerRecord = {
        topic,
        value: personRecord1
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-07",
        clientId: "data-binding-producer-07"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    Person[] receivedValues = [];
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        Person person = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(person);
    });
    test:assertEquals(receivedValues, [personRecord1, personRecord1, personRecord1]);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testMapProduce() returns error? {
    string topic = "map-produce-test-topic";
    kafkaTopics.push(topic);
    MapProducerRecord producerRecord = {
        topic,
        value: personMap
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-08",
        clientId: "data-binding-producer-08"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    map<Person>[] receivedValues = [];
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        map<Person> personMap = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(personMap);
    });
    test:assertEquals(receivedValues, [personMap, personMap, personMap]);
    check consumer->close();
}

@test:Config {
    groups: ["producer", "data-binding"]
}
function testTableProduce() returns error? {
    string topic = "table-produce-test-topic";
    kafkaTopics.push(topic);
    table<Person> personMapTable = table [];
    personMapTable.add(personRecord1);
    TableProducerRecord producerRecord = {
        topic,
        value: personMapTable
    };
    check producer->send(producerRecord);
    check producer->send(producerRecord);
    check producer->send(producerRecord);

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-producer-group-09",
        clientId: "data-binding-producer-09"
    };
    Consumer consumer = check new (DEFAULT_URL, consumerConfiguration);
    BytesConsumerRecord[] consumerRecords = check consumer->poll(3);
    test:assertEquals(consumerRecords.length(), 3);

    table<Person>[] receivedValues = [];
    consumerRecords.forEach(function(BytesConsumerRecord cRecord) {
        table<Person> personTable = checkpanic value:fromJsonStringWithType(checkpanic 'string:fromBytes(cRecord.value));
        receivedValues.push(personTable);
    });
    test:assertEquals(receivedValues, [personMapTable, personMapTable, personMapTable]);
    check consumer->close();
}
