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

// Topic to publish the filtered tweets.
const TOPIC = "twitter-tweets";

// User-defined tweet record type.
type Tweet record {|
    string created_at;
    int id;
    string text;
|};

// Creates a Kafka producer to publish filtered tweets to Kafka.
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

public function main() returns error? {

    // Creates a new HTTP client for the mock twitter server.
    final http:Client twitterClient = check new ("http://localhost:9090");

    // Get tweets from the mock twitter server.
    json[] response = <json[]> check twitterClient->get("/tweets");

    // Filters and publishes tweets to Kafka.
    check publishFilteredTweets(response);
}

function publishFilteredTweets(json[] tweets) returns error? {
    foreach var t in tweets {
        json jsonTweet = t.cloneReadOnly();
        Tweet tweet = <Tweet> jsonTweet;

        // Filter tweets with the id greater than 50000.
        if (tweet.id > 50000) {
            string message = jsonTweet.toJsonString();
            check kafkaProducer->send({ topic: TOPIC, value: message.toBytes() });
        }
    }
}
