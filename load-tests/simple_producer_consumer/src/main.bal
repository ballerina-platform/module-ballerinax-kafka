// Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import ballerina/lang.runtime;
import ballerina/http;
import ballerina/time;

const string MESSAGE = "Hello";
const string TOPIC = "perf-topic";
const string KAFKA_CLUSTER = "kafka:9092";
const int MESSAGE_COUNT = 10000;

int errorCount = 0;
int msgCount = 0;
time:Utc startedTime = time:utcNow();
time:Utc endedTime = time:utcNow();
boolean finished = false;

service /kafka on new http:Listener(9100) {

    resource function get publish() returns boolean {
        error? result = startListener();
        if result is error {
            return false;
        }
        errorCount = 0;
        msgCount = 0;
        startedTime = time:utcNow();
        endedTime = time:utcNow();
        finished = false;
        _ = start publishMessages();
        return true;
    }

    resource function get getResults() returns boolean|map<string> {
        if finished {
            return {
                errorCount: errorCount.toString(),
                time: time:utcDiffSeconds(endedTime, startedTime).toString(),
                sentCount: MESSAGE_COUNT.toString(),
                receivedCount: msgCount.toString()
            };
        }
        return false;
    }
}

function publishMessages() returns time:Seconds|error {
    startedTime = time:utcNow();
    kafka:Producer producer = check new(KAFKA_CLUSTER);
    foreach int i in 0...MESSAGE_COUNT {
        error? result = producer->send({
            value: MESSAGE.toBytes(),
            topic: TOPIC
        });
        if result is error {
            lock {
                errorCount += 1;
            }
        }
    }
    return time:utcDiffSeconds(endedTime, startedTime);
}

function startListener() returns error? {
    kafka:ConsumerConfiguration consumerConfigs = {
        groupId: "consumer",
        topics: [TOPIC],
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        pollingInterval: 1
    };
    kafka:Listener kafkaListener = check new (KAFKA_CLUSTER, consumerConfigs);
    check kafkaListener.attach(kafkaService);
    check kafkaListener.start();
    runtime:registerListener(kafkaListener);
}

kafka:Service kafkaService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string|error messageContent = 'string:fromBytes(consumerRecord.value);
            if messageContent !is string || messageContent != MESSAGE {
                lock {
                    errorCount += 1;
                }
            } else {
                msgCount += 1;
            }
        }
        if errorCount + msgCount >= MESSAGE_COUNT {
            finished = true;
            endedTime = time:utcNow();
        }
    }
};
