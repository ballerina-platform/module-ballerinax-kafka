// Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/kafka;
import ballerina/test;
import ballerina/io;

int retrievedRecordsCount = 0;
string receivedMessage = "";

handle? kafkaCluster = ();

@test:BeforeSuite
function startKafkaServer() returns error? {
    var result = createKafkaCluster();
    if (result is error) {
        io:println(result);
    } else {
        io:println("******* Started Kafka Server");
    }
}

@test:AfterSuite
function stopKafkaServer() returns error? {
    var result = stopKafkaCluster();
    if (result is error) {
        io:println(result);
    } else {
        io:println("******* Stopped Kafka Server");
    }
}

string topic1 = "consumer-unsubscribe-test-1";
string topic2 = "consumer-unsubscribe-test-2";

@test:Config {}
function testConsumer() {
    kafka:ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        topics: [topic1],
        groupId: "test-group-1",
        clientId: "test-consumer-1"
    };
    kafka:Consumer consumer = new(consumerConfiguration);
}

@test:Config {}
function testProducer() returns error? {
    kafka:ProducerConfiguration producerConfiguration = {
        bootstrapServers: "localhost:9092",
        clientId: "basic-producer",
        acks: kafka:ACKS_ALL,
        maxBlock: 6000,
        requestTimeoutInMillis: 2000,
        valueSerializerType: kafka:SER_STRING,
        retryCount: 3
    };
    kafka:Producer producer = new(producerConfiguration);
    string message = "Hello, Ballerina";
    return producer->send(message, topic1);
}

//function testTestUnsubscribe() returns boolean {
//    kafka:Consumer kafkaConsumer = new ({
//        bootstrapServers: "localhost:14101",
//        groupId: "test-group",
//        clientId: "unsubscribe-consumer",
//        topics: [topic1, topic2]
//    });
//    var subscribedTopics = kafkaConsumer->getSubscription();
//    if (subscribedTopics is error) {
//        return false;
//    }
//    else {
//        if (subscribedTopics.length() != 2) {
//            return false;
//        }
//    }
//    var result = kafkaConsumer->unsubscribe();
//    subscribedTopics = kafkaConsumer->getSubscription();
//    if (subscribedTopics is error) {
//        return false;
//    } else {
//        if (subscribedTopics.length() != 0) {
//            return false;
//        }
//        return true;
//    }
//}
