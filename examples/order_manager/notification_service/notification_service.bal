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
import ballerina/lang.value;
import ballerina/log;
import notification_service.types;

configurable string LISTENING_TOPIC = "success-orders";

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "notification-consumer",
    topics: [LISTENING_TOPIC],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

service kafka:Service on kafkaListener {

    // Listens to Kafka topic for any successful orders
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach kafka:ConsumerRecord 'record in records {

            // Convert the byte values in the Kafka record to type Order
            string messageContent = check string:fromBytes('record.value);
            json content = check value:fromJsonString(messageContent);
            json jsonTweet = content.cloneReadOnly();
            types:Order neworder = <types:Order> jsonTweet;
            log:printInfo("We have successfully ordered and going to send success message: " + neworder.toString());
        }
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer.", 'error = commitResult);
        }
    }
}
