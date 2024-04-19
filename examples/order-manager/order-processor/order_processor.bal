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

configurable string LISTENING_TOPIC = ?;
configurable string PUBLISH_TOPIC = ?;

// Creates a Kafka producer with default configurations
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "processing-consumer",
    topics: [LISTENING_TOPIC],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

service kafka:Service on kafkaListener {

    // Listens to Kafka topic for any new orders and process them
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        // Uses Ballerina query expressions to filter out the successful orders and publish to Kafka topic
        error? err = from types:Order 'order in check getOrdersFromRecords(records) where 'order.status == types:SUCCESS do {
            log:printInfo("Sending successful order to " + PUBLISH_TOPIC + " " + 'order.toString());
            check kafkaProducer->send({ topic: PUBLISH_TOPIC, value: 'order.toString().toBytes()});
        };
        if err is error {
            log:printError("Unknown error occured", err);
        }
    }
}

// Convert the byte values in Kafka records to type Order[]
function getOrdersFromRecords(kafka:BytesConsumerRecord[] records) returns types:Order[]|error {
    types:Order[] receivedOrders = [];
    foreach kafka:BytesConsumerRecord 'record in records {
        string messageContent = check string:fromBytes('record.value);
        json jsonContent = check value:fromJsonString(messageContent);
        json jsonClone = jsonContent.cloneReadOnly();
        types:Order receivedOrder = check jsonClone.ensureType(types:Order);
        receivedOrders.push(receivedOrder);
    }
    return receivedOrders;
}
