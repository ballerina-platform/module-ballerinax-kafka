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

import ballerinax/kafka;

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "group-id",
    topics: ["test-kafka-topic"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, int[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, byte[][] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, string[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, Person[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, map<Person>[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, table<Person>[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, json[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, xml[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, anydata[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, int[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, byte[][] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:Caller caller, string[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:Caller caller, Person[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(map<Person>[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & table<Person>[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, kafka:Caller caller, json[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, kafka:Caller caller, xml[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, anydata[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

public type Person record {|
    string name;
    int age;
    boolean married;
|};
