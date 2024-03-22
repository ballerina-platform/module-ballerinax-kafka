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

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, @kafka:Payload PayloadConsumerRecord[] data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller, @kafka:Payload PayloadConsumerRecord[] & readonly data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] & readonly records, kafka:Caller caller, @kafka:Payload PayloadConsumerRecord[] & readonly data) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecord[] & readonly data, kafka:BytesConsumerRecord[] records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecord[] & readonly data, kafka:BytesConsumerRecord[] & readonly records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecord[] & readonly data, PersonConsumerRecord[] & readonly records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecordWithTypeReference[] & readonly data, PersonConsumerRecord[] & readonly records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(string[] & readonly data, PersonConsumerRecord[] & readonly records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(string[] & readonly data, kafka:BytesConsumerRecord[] & readonly records, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecord[] payload, kafka:Caller caller) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

service kafka:Service on kafkaListener {
    private final string var1 = "Kafka Service";
    private final int var2 = 54;

    remote function onConsumerRecord(@kafka:Payload PayloadConsumerRecord[] payload) {
    }

    remote function onError(kafka:Error 'error) returns error|() {
    }
}

public type PersonConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    Person value;
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
    map<byte[]|byte[][]> headers;
|};

public type PayloadConsumerRecordWithTypeReference record {|
    string key?;
    string value;
    int timestamp;
    kafka:PartitionOffset offset;
    map<byte[]|byte[][]> headers;
|};

public type Person record {|
    string name;
    boolean isMarried;
    int age;
|};
