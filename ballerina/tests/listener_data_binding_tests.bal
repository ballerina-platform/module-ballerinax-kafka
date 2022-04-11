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
boolean errorReceived = false;
string errorMsg = "";

public type IntConsumerRecord record {|
    int key?;
    int value;
    int timestamp;
    PartitionOffset offset?;
|};

public type FloatConsumerRecord record {|
    float key?;
    float value;
    int timestamp;
    PartitionOffset offset?;
|};

public type DecimalConsumerRecord record {|
    decimal key?;
    decimal value;
    int timestamp;
    PartitionOffset offset?;
|};

public type BooleanConsumerRecord record {|
    boolean key?;
    boolean value;
    int timestamp;
    PartitionOffset offset?;
|};

public type StringConsumerRecord record {|
    string key?;
    string value;
    int timestamp;
    PartitionOffset offset?;
|};

public type PersonConsumerRecord record {|
    byte[] key?;
    Person value;
    int timestamp;
    PartitionOffset offset?;
|};

public type MapConsumerRecord record {|
    byte[] key?;
    map<Person> value;
    int timestamp;
    PartitionOffset offset?;
|};

public type XmlConsumerRecord record {|
    xml key?;
    xml value;
    int timestamp;
    PartitionOffset offset?;
|};

public type TableConsumerRecord record {|
    string key?;
    table<Person> value;
    int timestamp;
    PartitionOffset offset?;
|};

public type JsonConsumerRecord record {|
    json key?;
    json value;
    int timestamp;
    PartitionOffset offset?;
|};

@test:Config {enable: true}
function dataBindingErrorListenerTest() returns error? {
    string topic = "data-binding-error-listener-test-topic";
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
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(dataBindingErrorService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertTrue(errorReceived);
    test:assertTrue(errorMsg.startsWith("Data binding failed: "));
}

@test:Config {enable: true}
function intConsumerRecordBindingListenerTest() returns error? {
    string topic = "int-consumer-record-listener-test-topic";
    check sendMessage(1, topic);

    Service intBindingService =
    service object {
        remote function onConsumerRecord(readonly & IntConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function floatConsumerRecordBindingListenerTest() returns error? {
    string topic = "float-consumer-record-listener-test-topic";
    check sendMessage(10.5, topic);

    Service floatBindingService =
    service object {
        remote function onConsumerRecord(FloatConsumerRecord[] & readonly records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function decimalConsumerRecordBindingListenerTest() returns error? {
    string topic = "decimal-consumer-record-listener-test-topic";
    check sendMessage(98.5d, topic);

    Service decimalBindingService =
    service object {
        remote function onConsumerRecord(DecimalConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function booleanConsumerRecordBindingListenerTest() returns error? {
    string topic = "int-consumer-record-listener-test-topic";
    check sendMessage(true, topic);

    Service booleanBindingService =
    service object {
        remote function onConsumerRecord(BooleanConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function stringConsumerRecordListenerTest() returns error? {
    string topic = "string-consumer-record-listener-test-topic";
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);

    Service stringBindingService =
    service object {
        remote function onConsumerRecord(StringConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function xmlConsumerRecordListenerTest() returns error? {
    string topic = "xml-consumer-record-listener-test-topic";
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);

    Service xmlBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, XmlConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function recordConsumerRecordListenerTest() returns error? {
    string topic = "record-consumer-record-listener-test-topic";
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service recordBindingService =
    service object {
        remote function onConsumerRecord(PersonConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function mapConsumerRecordListenerTest() returns error? {
    string topic = "map-consumer-record-listener-test-topic";
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);

    Service mapBindingService =
    service object {
        remote function onConsumerRecord(MapConsumerRecord[] records, Caller caller) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function tableConsumerRecordListenerTest() returns error? {
    string topic = "table-consumer-record-listener-test-topic";
    table<Person> personMapTable = table [];

    personMapTable.add(personRecord1);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);

    Service tableBindingService =
    service object {
        remote function onConsumerRecord(TableConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() {
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

@test:Config {enable: true}
function jsonConsumerRecordListenerTest() returns error? {
    string topic = "json-consumer-record-listener-test-topic";
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service jsonBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, JsonConsumerRecord[] records) returns error? {
            foreach int i in 0 ... records.length() {
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
