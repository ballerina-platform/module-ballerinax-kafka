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

// Creates a Kafka producer with default configurations
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

    // Listens to new order requests and publishes the order to Kafka topic
    resource function get publish(string message, string status) returns string|error? {
        // Converts the request parameter status to type PaymentStatus
        types:PaymentStatus paymentStatus = check status.ensureType(types:PaymentStatus);

        // Create an order with a random id
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

    // Publish the order to the Kafka topic
    check kafkaProducer->send({ topic: TOPIC, value: 'order.toString().toBytes()});
}
