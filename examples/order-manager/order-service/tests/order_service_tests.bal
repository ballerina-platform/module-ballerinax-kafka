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

import order_service.types;

import ballerina/http;
import ballerina/lang.value;
import ballerina/test;
import ballerinax/kafka;
import ballerina/io;

configurable string username = "user";
configurable string password = "password";

@test:Config {}
function orderServiceTest() returns error? {
    http:Client orderClient = check new ("http://localhost:9090",
        auth = {
            username,
            password
        }
    );

    string orderName = "PS5";
    string orderStatus = "SUCCESS";

    io:println("Requesting order from the order service");
    string response = check orderClient->get("/kafka/publish?message=PS5&status=SUCCESS");
    string expectedResponse = string `Message sent to the Kafka topic ${TOPIC} successfully. Order ${orderName} with status ${orderStatus}`;
    test:assertEquals(response, expectedResponse);

    kafka:ConsumerConfiguration testConsumerConfigs = {
        groupId: "order-service-consumer",
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        topics: [TOPIC]
    };

    kafka:Consumer testConsumer = check new (kafka:DEFAULT_URL, testConsumerConfigs);
    kafka:BytesConsumerRecord[] records = check testConsumer->poll(5);
    test:assertEquals(records.length(), 1);

    string messageContent = check string:fromBytes(records[0].value);
    json jsonContent = check value:fromJsonString(messageContent);
    types:Order newOrder = check jsonContent.fromJsonWithType(types:Order);

    test:assertEquals(newOrder.name, orderName);
    test:assertEquals(newOrder.status, orderStatus);
}
