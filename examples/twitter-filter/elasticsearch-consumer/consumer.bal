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
import example/esConsumer.types as types;

// Topic to which the filtered tweets are published.
configurable string TOPIC = "twitter-tweets";

// The URL of the Elasticsearch instance to use for all your queries.
configurable string URL = "http://localhost:9200";

// Basic authentication details to connect to the Elasticsearch instance.
configurable string username = "username";
configurable string password = "password";

// Creates a new HTTP client for the Elasticsearch node.
final http:Client elkClient = check new (URL,
    auth = {
        username: username,
        password: password
    }
);

public function main() returns error? {
    // Creates the Elasticsearch index named `twitter`.
    string indexCreation = check elkClient->put("/twitter", ());
    log:printInfo(indexCreation);

    // Checks the status of the available indices.
    string resp = check elkClient->get("/_cat/indices?v");

    log:printInfo(resp);

    kafka:ConsumerConfiguration consumerConfigs = {
        groupId: "elastic-search",
        topics: [TOPIC],
        pollingInterval: 1,
        // Sets the `autoCommit` to `false` so that the records should be committed manually.
        autoCommit: false
    };

    kafka:Listener kafkaListener = check new (kafka:DEFAULT_URL, consumerConfigs);

    // Attaches the `consumerService` to the `kafkaListener`.
    check kafkaListener.attach(consumerService);
    // Starts the registered services.
    check kafkaListener.'start();
}

kafka:Service consumerService =
service object {
    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:BytesConsumerRecord[] records) returns error? {
        // The set of tweets received by the service are processed one by one.
        types:Tweet[] convertedTweets =check getTweetsFromConsumerRecords(records);

        // Filters tweets with the ID greater than 50000.
        types:Tweet[] filteredTweets = from var tweet in convertedTweets where tweet.id > 50000 select tweet;

        foreach var filteredTweet in filteredTweets {
            // Sends the JSON value of the tweet to the `twitter` Elasticsearch index.
            string payload = check elkClient->post("/twitter/tweets", filteredTweet.toJson());
            log:printInfo(payload);
        }

        // Commits offsets of the returned records by marking them as consumed.
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer.", 'error = commitResult);
        }
    }
};

function getTweetsFromConsumerRecords(kafka:BytesConsumerRecord[] records) returns types:Tweet[]|error {
    types:Tweet[] tweets = [];
    foreach int i in 0 ..< records.length() {
        kafka:BytesConsumerRecord kafkaRecord = records[i];
        // The value should be a `byte[]` since the byte[] deserializer is used for the value.
        byte[] value = kafkaRecord.value;

        // Converts the `byte[]` to a `string`.
        string messageContent = check string:fromBytes(value);
        json content = check value:fromJsonString(messageContent);
        json jsonTweet = content.cloneReadOnly();
        tweets[i] = <types:Tweet> jsonTweet;
    }
    return tweets;
}
