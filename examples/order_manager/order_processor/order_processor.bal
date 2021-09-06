// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import order_processor.types;
import ballerina/lang.value;
import ballerina/log;

configurable string LISTENING_TOPIC = "orders";
configurable string PUBLISH_TOPIC = "success-orders";

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "processing-consumer",
    topics: [LISTENING_TOPIC],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener =
        new (kafka:DEFAULT_URL, consumerConfigs);

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:ConsumerRecord[] records) returns error? {
        foreach kafka:ConsumerRecord 'record in records {
            log:printInfo("Received order " + 'record.toString());
            string messageContent = check string:fromBytes('record.value);
            json jsonContent = check value:fromJsonString(messageContent);
            json jsonClone = jsonContent.cloneReadOnly();
            types:Order receivedOrder = <types:Order> jsonClone;
            check processOrderAndPublish(receivedOrder);
        }
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer.", 'error = commitResult);
        }
    }
}

function processOrderAndPublish(types:Order 'order) returns error? {
    if 'order.status is types:SUCCESS {
        log:printInfo("Publishing successful order to topic " + PUBLISH_TOPIC);
        check kafkaProducer->send({ topic: PUBLISH_TOPIC, value: 'order.toString().toBytes()});
    } else {
        log:printInfo("Ignoring unsuccessful order: " + 'order.toString());
    }
}
