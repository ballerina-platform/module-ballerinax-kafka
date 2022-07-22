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
configurable string SASL_USERNAME = ?;
configurable string SASL_PASSWORD = ?;

// Truststore path and password
crypto:TrustStore trustStore = {
    path: SSL_TRUSTSTORE_PATH,
    password: SSL_MASTER_PASSWORD
};

// Keystore path and password
crypto:KeyStore keyStore = {
    path: SSL_KEYSTORE_PATH,
    password: SSL_MASTER_PASSWORD
};

// Configs related to secure socket record
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

// SASL authentication configurations
kafka:AuthenticationConfiguration authConfig = {
    mechanism: kafka:AUTH_SASL_PLAIN,
    username: SASL_USERNAME,
    password: SASL_PASSWORD
};

kafka:ProducerConfiguration producerConfigs = {
    clientId: "word-count-producer",
    acks: kafka:ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3,
    // Security configs
    auth: authConfig,
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "word-processing-consumer",
    topics: [INPUT_TOPIC],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1,
    // Security configs
    secureSocket: socket,
    securityProtocol: kafka:PROTOCOL_SASL_SSL,
    auth: authConfig,
    // Making autocommit false in order to re-evaluate the word counts for every new instance of the listener
    autoCommit: false
};

service kafka:Service on new kafka:Listener(KAFKA_SECURED_URL, consumerConfigs) {

    final kafka:Producer kafkaProducer;
    final map<int> wordCountMap = {};

    public function init() returns error? {
        self.kafkaProducer = check new (KAFKA_SECURED_URL, producerConfigs);
    }

    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        map<int> countResults;
        foreach kafka:ConsumerRecord 'record in records {
            // Process the input sentences one by one
            countResults = check processRecord(self.wordCountMap, 'record);
        }
        // Iterate through the results and publish the word with the appropriate count to the output topic
        foreach string word in countResults.keys() {
            int? count = countResults[word];
            if count is int {
                publishWordCount(self.kafkaProducer, word, count);
            }
        }
    }
}

function processRecord(map<int> wordCountMap, kafka:ConsumerRecord 'record) returns map<int>|error {
    // A temporary word count map is created to count the words in one sentence
    map<int> tempWordCountMap = {};
    string sentence = check string:fromBytes('record.value);

    _ = check from string word in regex:split(sentence, " ") let int? result = wordCountMap[word] do {
        if result is () {
            // Add a new value if the word does not exist in the map
            wordCountMap[word] = 1;
            tempWordCountMap[word] = 1;
        } else {
            // Increment the count if the word already exists
            wordCountMap[word] = result + 1;
            tempWordCountMap[word] = result + 1;
        }
    };
    return tempWordCountMap;
}

function publishWordCount(kafka:Producer kafkaProducer, string word, int count) {
    // Publish the message to the topic with the count as the message value and the word as the message key
    error? result = kafkaProducer->send({ topic: OUTPUT_TOPIC, 'key: word.toBytes(), value: count.toString().toBytes() });
    if result is error {
        log:printError("Could not send word " + word + " with count " + count.toString() + " to kafka", result);
    }
}
