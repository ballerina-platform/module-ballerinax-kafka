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

import ballerina/lang.runtime as runtime;
import ballerina/log;
import ballerina/test;

public type Person record {|
    readonly string name;
    int age;
    string address;
    boolean married;
|};

Person personRecord1 = {
    name: "Anne",
    age: 25,
    address: "Palm Grove",
    married: false
};

Person personRecord2 = {
    name: "Jane",
    address: "Unknown",
    age: 12,
    married: false
};

Person personRecord3 = {
    name: "John",
    address: "Col-10",
    age: 22,
    married: true
};

map<Person> personMap = {
    "P1": personRecord1,
    "P2": personRecord2,
    "P3": personRecord3
};

json jsonData = personMap.toJson();

int receivedIntValue = 0;
float receivedFloatValue = 0;
decimal receivedDecimalValue = 0;
boolean receivedBooleanValue = false;
string receivedStringValue = "";
xml receivedXmlValue = xml ``;
Person? receivedPersonValue = ();
map<Person> receivedMapValue = {};
table<Person> receivedTableValue = table [];
json receivedJsonValue = {};
int receivedIntPayload = 0;
float receivedFloatPayload = 0;
decimal receivedDecimalPayload = 0;
boolean receivedBooleanPayload = false;
string receivedStringPayload = "";
xml receivedXmlPayload = xml ``;
Person? receivedPersonPayload = ();
map<Person> receivedMapPayload = {};
table<Person> receivedTablePayload = table [];
json receivedJsonPayload = {};
json receivedPayloadConsumerRecordValue = {};
boolean errorReceived = false;
string errorMsg = "";
boolean isConsumerRecordReadonly = false;
boolean isPayloadReadonly = false;
anydata[] readOnlyPayloads = [];
int receivedSeekedValidRecordListenerCount = 0;
string recordCastErrorConsumerRecordError = "";
string recordCastErrorPayloadError = "";
boolean receivedAutoSeekError = false;
int receivedAutoSeekPayloadValue = 0;

public type IntConsumerRecord record {|
    int key?;
    int value;
    int timestamp;
    PartitionOffset offset;
    map<byte[]|byte[][]> headers;
|};

public type FloatConsumerRecord record {|
    *AnydataConsumerRecord;
    float key?;
    float value;
|};

public type DecimalConsumerRecord record {|
    *AnydataConsumerRecord;
    decimal value;
    decimal key?;
|};

public type BooleanConsumerRecord record {|
    *AnydataConsumerRecord;
    boolean key?;
    boolean value;
|};

public type StringConsumerRecord record {|
    *AnydataConsumerRecord;
    string key?;
    string value;
|};

public type PersonConsumerRecord record {|
    *AnydataConsumerRecord;
    Person value;
|};

public type MapConsumerRecord record {|
    *AnydataConsumerRecord;
    byte[] key?;
    map<Person> value;
|};

public type XmlConsumerRecord record {|
    *AnydataConsumerRecord;
    xml key?;
    xml value;
|};

public type TableConsumerRecord record {|
    string key?;
    table<Person> value;
    int timestamp;
    PartitionOffset offset;
    map<byte[]|byte[][]> headers;
|};

public type JsonConsumerRecord record {|
    PartitionOffset offset;
    json key?;
    int timestamp;
    json value;
    map<byte[]|byte[][]> headers;
|};

public type PayloadConsumerRecord record {|
    string key?;
    string value;
    int timestamp;
    record {|
        int offset;
        record {|
            string topic;
            int partition;
        |} partition;
    |} offset;
    map<byte[]|byte[][]> headers?;
|};

PayloadConsumerRecord payloadConsumerRecord = {
    key: "test-key",
    offset: {
        offset: 12,
        partition: {
            topic: "test-topic",
            partition: 2
        }
    },
    timestamp: 124125124,
    value: "test-value"
};

@test:Config {
    groups: ["listener", "data-binding"]
}
function testDataBindingErrorListener() returns error? {
    string topic = "data-binding-error-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service dataBindingErrorService =
    service object {
        remote function onConsumerRecord(readonly & XmlConsumerRecord[] records, Caller caller) returns error? {
            foreach XmlConsumerRecord 'record in records {
                log:printInfo("Received int record: " + 'record.toString());
                errorReceived = false;
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
            errorReceived = true;
            errorMsg = e.message();
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-11",
        clientId: "data-binding-listener-11",
        pollingInterval: 1,
        autoSeekOnValidationFailure: false
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(dataBindingErrorService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertTrue(errorReceived);
    test:assertTrue(errorMsg.startsWith("Data binding failed."));
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testIntConsumerRecordBindingListener() returns error? {
    string topic = "int-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(1, topic);

    Service intBindingService =
    service object {
        remote function onConsumerRecord(readonly & IntConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedIntValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(intBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedIntValue, 1);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testFloatConsumerRecordBindingListener() returns error? {
    string topic = "float-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(10.5, topic);

    Service floatBindingService =
    service object {
        remote function onConsumerRecord(FloatConsumerRecord[] & readonly records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedFloatValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(floatBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedFloatValue, 10.5);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testDecimalConsumerRecordBindingListener() returns error? {
    string topic = "decimal-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(98.5d, topic);

    Service decimalBindingService =
    service object {
        remote function onConsumerRecord(DecimalConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedDecimalValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(decimalBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedDecimalValue, 98.5d);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testBooleanConsumerRecordBindingListener() returns error? {
    string topic = "boolean-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(true, topic);

    Service booleanBindingService =
    service object {
        remote function onConsumerRecord(BooleanConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedBooleanValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(booleanBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedBooleanValue, true);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testStringConsumerRecordListener() returns error? {
    string topic = "string-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);

    Service stringBindingService =
    service object {
        remote function onConsumerRecord(StringConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedStringValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-04",
        clientId: "data-binding-listener-04",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(stringBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedStringValue, TEST_MESSAGE);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testXmlConsumerRecordListener() returns error? {
    string topic = "xml-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);

    Service xmlBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, XmlConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedXmlValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-06",
        clientId: "data-binding-listener-06",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(xmlBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedXmlValue, xmlData);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testRecordConsumerRecordListener() returns error? {
    string topic = "record-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service recordBindingService =
    service object {
        remote function onConsumerRecord(PersonConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedPersonValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-07",
        clientId: "data-binding-listener-07",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(recordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedPersonValue, personRecord1);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testMapConsumerRecordListener() returns error? {
    string topic = "map-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);

    Service mapBindingService =
    service object {
        remote function onConsumerRecord(MapConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedMapValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-07",
        clientId: "data-binding-listener-07",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(mapBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedMapValue, personMap);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testTableConsumerRecordListener() returns error? {
    string topic = "table-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    table<Person> personMapTable = table [];

    personMapTable.add(personRecord1);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);

    Service tableBindingService =
    service object {
        remote function onConsumerRecord(TableConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedTableValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-09",
        clientId: "data-binding-listener-09",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(tableBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedTableValue, personMapTable);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testJsonConsumerRecordListener() returns error? {
    string topic = "json-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service jsonBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, JsonConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                receivedJsonValue = records[i].value;
                log:printInfo("Received record: " + records[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(jsonBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedJsonValue, jsonData);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testReadonlyConsumerRecordListener() returns error? {
    string topic = "readonly-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service readonlyBindingService =
    service object {
        remote function onConsumerRecord(PersonConsumerRecord[] & readonly records, Caller caller) returns error? {
            foreach PersonConsumerRecord rec in records {
                if rec.isReadOnly() {
                    isConsumerRecordReadonly = true;
                } else {
                    isConsumerRecordReadonly = false;
                }
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-07",
        clientId: "data-binding-listener-07",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(readonlyBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertTrue(isConsumerRecordReadonly);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testIntPayloadBindingListener() returns error? {
    string topic = "int-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(1, topic);

    Service intBindingService =
    service object {
        remote function onConsumerRecord(int[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedIntPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(intBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedIntPayload, 1);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testFloatPayloadBindingListener() returns error? {
    string topic = "float-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(10.5, topic);

    Service floatBindingService =
    service object {
        remote function onConsumerRecord(float[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedFloatPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(floatBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedFloatPayload, 10.5);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testDecimalPayloadBindingListener() returns error? {
    string topic = "decimal-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(98.5d, topic);

    Service decimalBindingService =
    service object {
        remote function onConsumerRecord(decimal[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedDecimalPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(decimalBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedDecimalPayload, 98.5d);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testBooleanPayloadBindingListener() returns error? {
    string topic = "boolean-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(true, topic);

    Service booleanBindingService =
    service object {
        remote function onConsumerRecord(boolean[] payload, Caller caller) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedBooleanPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-01",
        clientId: "data-binding-listener-01",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(booleanBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedBooleanPayload, true);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testStringPayloadListener() returns error? {
    string topic = "string-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);

    Service stringBindingService =
    service object {
        remote function onConsumerRecord(StringConsumerRecord[] records, string[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedStringPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-04",
        clientId: "data-binding-listener-04",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(stringBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedStringPayload, TEST_MESSAGE);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testXmlPayloadListener() returns error? {
    string topic = "xml-payload-listener-test-topic";
    kafkaTopics.push(topic);
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);

    Service xmlBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, xml[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedXmlPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-06",
        clientId: "data-binding-listener-06",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(xmlBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedXmlPayload, xmlData);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testRecordPayloadListener() returns error? {
    string topic = "record-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service recordBindingService =
    service object {
        remote function onConsumerRecord(Person[] payload, Caller caller, PersonConsumerRecord[] records) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedPersonPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-07",
        clientId: "data-binding-listener-07",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(recordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedPersonPayload, personRecord1);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testMapPayloadListener() returns error? {
    string topic = "map-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);

    Service mapBindingService =
    service object {
        remote function onConsumerRecord(map<Person>[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedMapPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-07",
        clientId: "data-binding-listener-07",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(mapBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedMapPayload, personMap);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testTablePayloadListener() returns error? {
    string topic = "table-payload-listener-test-topic";
    kafkaTopics.push(topic);
    table<Person> personMapTable = table [];

    personMapTable.add(personRecord1);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);

    Service tableBindingService =
    service object {
        remote function onConsumerRecord(table<Person>[] payload, TableConsumerRecord[] records) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedTablePayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-09",
        clientId: "data-binding-listener-09",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(tableBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedTablePayload, personMapTable);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testJsonPayloadListener() returns error? {
    string topic = "json-payload-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service jsonBindingService =
    service object {
        remote function onConsumerRecord(json[] payload) returns error? {
            foreach int i in 0 ... payload.length() - 1 {
                receivedJsonPayload = payload[i];
                log:printInfo("Received record: " + payload[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(jsonBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedJsonPayload, jsonData);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testPayloadConsumerRecordListener() returns error? {
    string topic = "payload-consumer-record-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(payloadConsumerRecord, topic);
    check sendMessage(payloadConsumerRecord, topic);
    check sendMessage(payloadConsumerRecord, topic);

    Service payloadRecordBindingService =
    service object {
        remote function onConsumerRecord(@Payload PayloadConsumerRecord[] payloadRecords, JsonConsumerRecord[] consumerRecords) returns error? {
            foreach int i in 0 ... payloadRecords.length() - 1 {
                receivedPayloadConsumerRecordValue = payloadRecords[i];
                log:printInfo("Received record: " + payloadRecords[i].toString());
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(payloadRecordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedPayloadConsumerRecordValue, payloadConsumerRecord);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testReadonlyPayloadListener() returns error? {
    string topic = "readonly-payload-listener-test-topic";
    kafkaTopics.push(topic);
    isPayloadReadonly = false;
    readOnlyPayloads = [];
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service payloadRecordBindingService =
    service object {
        remote function onConsumerRecord(Person[] & readonly payload, Caller caller, PersonConsumerRecord[] records) returns error? {
            if payload.isReadOnly() {
                readOnlyPayloads = payload;
                isPayloadReadonly = true;
            } else {
                isPayloadReadonly = false;
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(payloadRecordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertTrue(isPayloadReadonly);
    test:assertEquals(readOnlyPayloads, [personRecord1, personRecord1, personRecord1]);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testReadonlyPayloadWithPayloadAnnotationListener() returns error? {
    string topic = "readonly-payload-consumer-record-with-annotation-record-listener-test-topic";
    kafkaTopics.push(topic);

    isPayloadReadonly = false;
    readOnlyPayloads = [];

    check sendMessage(payloadConsumerRecord, topic);
    check sendMessage(payloadConsumerRecord, topic);
    check sendMessage(payloadConsumerRecord, topic);

    Service payloadRecordBindingService =
    service object {
        remote function onConsumerRecord(@Payload PayloadConsumerRecord[] & readonly payloadRecords, JsonConsumerRecord[] consumerRecords) returns error? {
            if payloadRecords.isReadOnly() {
                readOnlyPayloads = payloadRecords;
                isPayloadReadonly = true;
            } else {
                isPayloadReadonly = false;
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(payloadRecordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertTrue(isPayloadReadonly);
    test:assertEquals(readOnlyPayloads, [payloadConsumerRecord, payloadConsumerRecord, payloadConsumerRecord]);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testReadonlyPayloadReadonlyConsumerRecordsListener() returns error? {
    string topic = "readonly-payload-with-readonly-consumer-records-listener-test-topic";
    kafkaTopics.push(topic);

    isPayloadReadonly = false;
    readOnlyPayloads = [];

    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service payloadRecordBindingService =
    service object {
        remote function onConsumerRecord(Person[] & readonly payload, PersonConsumerRecord[] & readonly consumerRecords) returns error? {
            if payload.isReadOnly() && consumerRecords.isReadOnly() {
                readOnlyPayloads = payload;
                isPayloadReadonly = true;
            } else {
                isPayloadReadonly = false;
            }
        }

        remote function onError(Error e) {
            log:printError(e.message());
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-10",
        clientId: "data-binding-listener-10",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(payloadRecordBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertTrue(isPayloadReadonly);
    test:assertEquals(readOnlyPayloads, [personRecord1, personRecord1, personRecord1]);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testInvalidRecordPayloadWithSeekListener() returns error? {
    string topic = "invalid-record-payload-with-seek-listener-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(personRecord1, topic);
    check sendMessage("Invalid", topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service invalidRecordService =
    service object {
        remote function onConsumerRecord(PersonConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() - 1 {
                log:printInfo("Received record: " + records[i].toString());
                receivedSeekedValidRecordListenerCount += 1;
            }
        }

        remote function onError(Error e, Caller caller) returns error? {
            log:printError(e.toString());
            if e is PayloadBindingError {
                check caller->seek({
                    partition: e.detail().partition,
                    offset: e.detail().offset + 1
                });
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-11",
        clientId: "data-binding-listener-11",
        pollingInterval: 2
    };
    Listener payloadListener = check new (DEFAULT_URL, consumerConfiguration);
    check payloadListener.attach(invalidRecordService);
    check payloadListener.'start();
    runtime:sleep(5);
    check payloadListener.gracefulStop();
    test:assertEquals(receivedSeekedValidRecordListenerCount, 3);
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testRecordCastingErrorConsumerRecord() returns error? {
    string topic = "record-casting-error-consumer-record-test-topic";
    kafkaTopics.push(topic);
    check sendMessage({
                name: "ABC",
                age: 12,
                address: "test-address",
                married: false,
                id: 1231
            }.toString().toBytes(), topic);

    Service invalidRecordService =
    service object {
        remote function onConsumerRecord(PersonConsumerRecord[] records) returns error? {
        }

        remote function onError(Error e) returns error? {
            if e is PayloadBindingError {
                recordCastErrorConsumerRecordError = e.message();
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-12",
        clientId: "data-binding-listener-12",
        pollingInterval: 2,
        autoSeekOnValidationFailure: false
    };
    Listener recordListener = check new (DEFAULT_URL, consumerConfiguration);
    check recordListener.attach(invalidRecordService);
    check recordListener.'start();
    runtime:sleep(5);
    check recordListener.gracefulStop();
    test:assertEquals(recordCastErrorConsumerRecordError, "Data binding failed. If needed, please seek past the record to continue consumption.");
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testRecordCastingErrorPayload() returns error? {
    string topic = "record-casting-error-payload-test-topic";
    kafkaTopics.push(topic);
    check sendMessage({
                name: "ABC",
                age: 12,
                address: "test-address",
                married: false,
                id: 1231
            }.toString().toBytes(), topic);

    Service invalidRecordService =
    service object {
        remote function onConsumerRecord(Person[] records) returns error? {
        }

        remote function onError(Error e) returns error? {
            if e is PayloadBindingError {
                recordCastErrorPayloadError = e.message();
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-13",
        clientId: "data-binding-listener-13",
        pollingInterval: 2,
        autoSeekOnValidationFailure: false
    };
    Listener recordListener = check new (DEFAULT_URL, consumerConfiguration);
    check recordListener.attach(invalidRecordService);
    check recordListener.'start();
    runtime:sleep(5);
    check recordListener.gracefulStop();
    test:assertEquals(recordCastErrorPayloadError, "Data binding failed. If needed, please seek past the record to continue consumption.");
}

@test:Config {
    groups: ["listener", "data-binding"]
}
function testIntCastingErrorPayloadWithAutoSeek() returns error? {
    string topic = "int-casting-error-payload-with-auto-seek-test-topic";
    kafkaTopics.push(topic);
    check sendMessage(12.toString().toBytes(), topic);
    check sendMessage("Invalid msg".toBytes(), topic);
    check sendMessage(13.toString().toBytes(), topic);

    Service invalidIntService =
    service object {
        remote function onConsumerRecord(int[] values) returns error? {
            foreach int val in values {
                receivedAutoSeekPayloadValue += val;
            }
        }

        remote function onError(Error e) returns error? {
            receivedAutoSeekError = true;
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-14",
        clientId: "data-binding-listener-14",
        pollingInterval: 2
    };
    Listener intListener = check new (DEFAULT_URL, consumerConfiguration);
    check intListener.attach(invalidIntService);
    check intListener.'start();
    runtime:sleep(5);
    check intListener.gracefulStop();
    test:assertEquals(receivedAutoSeekPayloadValue, 25);
    test:assertFalse(receivedAutoSeekError);
}
