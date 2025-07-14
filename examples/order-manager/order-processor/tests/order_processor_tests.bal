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

import order_processor.types;

import ballerina/lang.runtime;
import ballerina/lang.value;
import ballerina/test;
import ballerinax/kafka;

@test:Config {}
function orderProcessorTest() returns error? {
    kafka:Producer testProducer = check new (kafka:DEFAULT_URL);

    types:Order 'order = {
        id: 1,
        name: "Test Order",
        status: types:SUCCESS
    };
    check testProducer->send({topic: LISTENING_TOPIC, value: 'order.toString().toBytes()});
    runtime:sleep(4);

    kafka:ConsumerConfiguration testConsumerConfigs = {
        groupId: "test-consumer",
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        topics: [PUBLISH_TOPIC]
    };
    kafka:Consumer testConsumer = check new (kafka:DEFAULT_URL, testConsumerConfigs);
    kafka:BytesConsumerRecord[] records = check testConsumer->poll(3);

    test:assertEquals(records.length(), 1);

    string messageContent = check string:fromBytes(records[0].value);
    json content = check value:fromJsonString(messageContent);
    json jsonTweet = content.cloneReadOnly();
    types:Order newOrder = check jsonTweet.ensureType();

    test:assertEquals(newOrder, 'order);
}
