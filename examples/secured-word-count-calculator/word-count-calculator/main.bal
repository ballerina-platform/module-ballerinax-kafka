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
import ballerina/log;
import ballerina/regex;
import ballerina/crypto;

configurable string INPUT_TOPIC = ?;
configurable string OUTPUT_TOPIC = ?;
configurable string SSL_TRUSTSTORE_PATH = ?;
configurable string SSL_KEYSTORE_PATH = ?;
configurable string SSL_MASTER_PASSWORD = ?;
configurable string KAFKA_SECURED_URL = ?;

map<int> wordCountMap = {};

crypto:TrustStore trustStore = {
    path: SSL_TRUSTSTORE_PATH,
    password: SSL_MASTER_PASSWORD
};

crypto:KeyStore keyStore = {
    path: SSL_KEYSTORE_PATH,
    password: SSL_MASTER_PASSWORD
};

kafka:SecureSocket socket = {
    cert: trustStore,
    key: {
        keyStore: keyStore,
        keyPassword: SSL_MASTER_PASSWORD
    },
    protocol: {
        name: kafka:SSL
    }
};

kafka:AuthenticationConfiguration authConfig = {
    mechanism: kafka:AUTH_SASL_PLAIN,
    username: "admin",
    password: "password"
};

kafka:ProducerConfiguration producerConfigs = {
    clientId: "word-count-producer",
    acks: kafka:ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3,
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "word-processing-consumer",
    topics: [INPUT_TOPIC],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL,
    auth: authConfig
};

listener kafka:Listener kafkaListener = new (KAFKA_SECURED_URL, consumerConfigs);

kafka:Producer kafkaProducer = check new (KAFKA_SECURED_URL, producerConfigs);

service kafka:Service on kafkaListener {

    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        check processRecords(records);
    }
}

function processRecords(kafka:ConsumerRecord[] records) returns error? {
    string sentence;
    foreach kafka:ConsumerRecord 'record in records {
        sentence = check string:fromBytes('record.value);
        _ = check from string word in regex:split(sentence, " ") let int? result = wordCountMap[word] do {
            if result is () {
                wordCountMap[word] = 1;
                future fut = start publishWordCount(word, 1);
            } else {
                wordCountMap[word] = result + 1;
                future fut = start publishWordCount(word, result + 1);
            }
        };
    }
}

function publishWordCount(string word, int count) {
    error? result = kafkaProducer->send({ topic: OUTPUT_TOPIC, 'key: word.toBytes(), value: count.toString().toBytes() });
    if result is error {
        log:printError("Could not send value to kafka", result);
    }
}
