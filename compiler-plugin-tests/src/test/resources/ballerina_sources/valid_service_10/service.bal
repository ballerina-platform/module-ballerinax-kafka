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

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & int[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & byte[][] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & string[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & Person[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & map<Person>[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & table<Person>[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & json[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & xml[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & anydata[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, readonly & int[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, readonly & byte[][] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:Caller caller, readonly & string[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:Caller caller, readonly & Person[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & map<Person>[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & table<Person>[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & json[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, kafka:Caller caller, readonly & xml[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {

    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(readonly & kafka:BytesConsumerRecord[] records, readonly & anydata[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

public type Person record {|
    string name;
    int age;
    boolean married;
|};
