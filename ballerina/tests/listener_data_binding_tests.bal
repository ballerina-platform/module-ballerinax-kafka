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
boolean receivedBoolValue = false;
string receivedStringValue = "";
decimal receivedDecimalValue = 0d;
float receivedfloatValue = 0;
xml[] receivedXmlValues = [];
json[] receivedJsonValues = [];
map<Person>[] receivedMapValues = [];
Person[] receivedPersonValues = [];
table<Person>[] receivedTableValues = [];
boolean errorReceived = false;
string errorMsg = "";

@test:Config {enable: true}
function intBindingListenerTest() returns error? {
    string topic = "int-binding-listener-test-topic";
    check sendMessage(1, topic);
    check sendMessage(2, topic);
    check sendMessage(3, topic);
    check sendMessage(4, topic);
    check sendMessage(5, topic);

    Service intBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, Caller caller, int[] data) returns error? {
            foreach int val in data {
                receivedIntValue += val;
                log:printInfo("Received int: " + val.toString());
            }
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
    test:assertEquals(receivedIntValue, 15);
}

@test:Config {enable: true}
function booleanBindingListenerTest() returns error? {
    string topic = "boolean-binding-listener-test-topic";
    check sendMessage(true, topic);
    check sendMessage(true, topic);
    check sendMessage(true, topic);

    Service booleanBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, Caller caller, boolean[] data) returns error? {
            foreach boolean value in data {
                receivedBoolValue = value;
                log:printInfo("Received boolean: " + value.toString());
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-02",
        clientId: "data-binding-listener-02",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(booleanBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedBoolValue, true);
}

@test:Config {enable: true}
function decimalBindingListenerTest() returns error? {
    string topic = "decimal-binding-listener-test-topic";
    check sendMessage(1.0d, topic);
    check sendMessage(2.0d, topic);
    check sendMessage(3.0d, topic);

    Service decimalBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, Caller caller, decimal[] data) returns error? {
            foreach decimal value in data {
                receivedDecimalValue += value;
                log:printInfo("Received decimal: " + value.toString());
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-03",
        clientId: "data-binding-listener-03",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(decimalBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedDecimalValue, 6.0d);
}

@test:Config {enable: true}
function stringBindingListenerTest() returns error? {
    string topic = "string-binding-listener-test-topic";
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);
    check sendMessage(TEST_MESSAGE, topic);

    Service decimalBindingService =
    service object {
        remote function onConsumerRecord(string[] data) returns error? {
            foreach string value in data {
                receivedStringValue += value;
                log:printInfo("Received string: " + value.toString());
            }
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
    check dataBindingListener.attach(decimalBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedStringValue, TEST_MESSAGE + TEST_MESSAGE + TEST_MESSAGE);
}

@test:Config {enable: true}
function floatBindingListenerTest() returns error? {
    string topic = "float-binding-listener-test-topic";
    check sendMessage(1.0, topic);
    check sendMessage(2.0, topic);
    check sendMessage(3.0, topic);

    Service floatBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, float[] data) returns error? {
            foreach float value in data {
                receivedfloatValue += value;
                log:printInfo("Received float: " + value.toString());
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-05",
        clientId: "data-binding-listener-05",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(floatBindingService);
    check dataBindingListener.'start();
    runtime:sleep(3);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedfloatValue, 6.0);
}

@test:Config {enable: true}
function xmlBindingListenerTest() returns error? {
    string topic = "xml-binding-listener-test-topic";
    xml xmlData = xml `<start><Person><name>wso2</name><location>col-03</location></Person><Person><name>wso2</name><location>col-03</location></Person></start>`;
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);
    check sendMessage(xmlData, topic);

    Service xmlBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, xml[] data) returns error? {
            foreach xml value in data {
                receivedXmlValues.push(value);
                log:printInfo("Received xml: " + value.toString());
            }
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
    test:assertEquals(receivedXmlValues, [xmlData, xmlData, xmlData]);
}

@test:Config {enable: true}
function recordBindingListenerTest() returns error? {
    string topic = "record-binding-listener-test-topic";
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);
    check sendMessage(personRecord1, topic);

    Service recordBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, Caller caller, Person[] data) returns error? {
            foreach Person value in data {
                receivedPersonValues.push(value);
                log:printInfo("Received record: " + value.toString());
            }
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
    test:assertEquals(receivedPersonValues, [personRecord1, personRecord1, personRecord1]);
}

@test:Config {enable: true}
function mapBindingListenerTest() returns error? {
    string topic = "map-binding-listener-test-topic";
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);
    check sendMessage(personMap, topic);

    Service mapBindingService =
    service object {
        remote function onConsumerRecord(map<Person>[] data) returns error? {
            foreach map<Person> value in data {
                receivedMapValues.push(value);
                log:printInfo("Received map: " + value.toString());
            }
        }
    };

    ConsumerConfiguration consumerConfiguration = {
        topics: [topic],
        offsetReset: OFFSET_RESET_EARLIEST,
        groupId: "data-binding-listener-group-08",
        clientId: "data-binding-listener-08",
        pollingInterval: 1
    };
    Listener dataBindingListener = check new (DEFAULT_URL, consumerConfiguration);
    check dataBindingListener.attach(mapBindingService);
    check dataBindingListener.'start();
    runtime:sleep(5);
    check dataBindingListener.gracefulStop();
    test:assertEquals(receivedMapValues, [personMap, personMap, personMap]);
}

@test:Config {enable: true}
function tableBindingListenerTest() returns error? {
    string topic = "table-binding-listener-test-topic";
    table<Person> personMapTable = table [];

    personMapTable.add(personRecord1);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);
    check sendMessage(personMapTable, topic);

    Service tableBindingService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, table<Person>[] data) returns error? {
            foreach table<Person> value in data {
                receivedTableValues.push(value);
                log:printInfo("Received table: " + value.toString());
            }
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
    table<Person>[] expectedValues = [personMapTable, personMapTable, personMapTable];
    test:assertEquals(receivedTableValues, expectedValues);
}

@test:Config {enable: true}
function jsonBindingListenerTest() returns error? {
    string topic = "json-binding-listener-test-topic";
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service jsonBindingService =
    service object {
        remote function onConsumerRecord(Caller caller, json[] data) returns error? {
            foreach json value in data {
                receivedJsonValues.push(value);
                log:printInfo("Received json: " + value.toString());
            }
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
    test:assertEquals(receivedJsonValues, [jsonData, jsonData, jsonData]);
}

@test:Config {enable: true}
function dataBindingErrorListenerTest() returns error? {
    string topic = "data-binding-error-listener-test-topic";
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);
    check sendMessage(jsonData, topic);

    Service dataBindingErrorService =
    service object {
        remote function onConsumerRecord(ConsumerRecord[] records, Caller caller, int[] data) returns error? {
            foreach json value in data {
                log:printInfo("Received json: " + value.toString());
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
