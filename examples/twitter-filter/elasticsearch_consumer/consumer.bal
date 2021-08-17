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
import ballerina/log;
import ballerinax/kafka;
import ballerina/lang.value;

// Topic to which the filtered tweets are published.
const TOPIC = "twitter-tweets";

// The URL of the Elasticsearch instance to use for all your queries.
string URL = "http://localhost:9200";

// Basic authentication details to connect to Elasticsearch instance.
string username = "username";
string password = "password";

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "elastic-search",
    topics: [TOPIC],
    pollingInterval: 1,
    // Sets the `autoCommit` to `false` so that the records should be committed manually.
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

// Creates a new HTTP client for Elasticsearch node.
final http:Client twitterClient = check new (URL,
auth = {
    username: username,
    password: password
});

public function main() returns error? {
    // Creates the Elasticsearch index named twitter.
    string indexCreation = check twitterClient->put("/twitter/tweets", ());
    log:printInfo(indexCreation);

    // Checks the status of the available indices.
    string resp = check twitterClient->get("/_cat/indices?v");
    log:printInfo(resp);

    // Attaches the `consumerService` to the listener, `kafkaListener`.
    check kafkaListener.attach(consumerService);
    // Starts the registered services.
    check kafkaListener.'start();
}

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:ConsumerRecord[] records) returns error? {
        // The set of tweets received by the service are processed one by one.
        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
        }

        // Commits offsets of the returned records by marking them as consumed.
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer.", 'error = commitResult);
        }
    }
}

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) returns error? {
    // The value should be a `byte[]` since the byte[] deserializer is used for the value.
    byte[] value = kafkaRecord.value;

    // Converts the `byte[]` to a `string`.
    string messageContent = check string:fromBytes(value);
    json content = check value:fromJsonString(messageContent);

    // Send the json value of the tweet to the Elasticsearch index, twitter.
    string response = check twitterClient->post("/twitter/tweets", content);
    log:printInfo(response);
}
