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

import ballerina/http;
import ballerinax/kafka;
import ballerina/log;
import order_service.types;
import ballerina/random;

configurable string TOPIC = "orders";
configurable int LISTENER_PORT = 9090;

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

@http:ServiceConfig {
    auth: [
        {
            fileUserStoreConfig: {},
            scopes: ["customer"]
        }
    ]
}
service /kafka on new http:Listener(LISTENER_PORT) {

    resource function get publish(string message, string status) returns string|error? {
        types:PaymentStatus paymentStatus = <types:PaymentStatus> status;
        types:Order randomOrder = {
            id: check random:createIntInRange(0, 100),
            name: message,
            status: paymentStatus
        };
        check publishOrder(randomOrder);
        return "Message sent to the Kafka topic " + TOPIC + " successfully. Order " + message + " with status " + paymentStatus;
    }
}

function publishOrder(types:Order 'order) returns error? {
    log:printInfo("Publishing order " + 'order.toString());
    check kafkaProducer->send({ topic: TOPIC, value: 'order.toString().toBytes()});
}
