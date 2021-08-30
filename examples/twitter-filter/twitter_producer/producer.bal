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
import example/twitterProducer.types as types;

// Topic to publish the filtered tweets.
const TOPIC = "twitter-tweets";

// Basic authentication details to connect to the mock twitter server.
configurable string username = "apiKey";
configurable string password = "apiSecret";

// Creates a Kafka producer to publish filtered tweets to Kafka.
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

public function main() returns error? {

    // Creates a new HTTP client for the mock Twitter server.
    final http:Client twitterClient = check new ("http://localhost:9090",
        auth = {
            username: username,
            password: password
        }
    );
    while(true) {
        // Gets tweets from the mock Twitter server.
        types:Tweet[] response = check twitterClient->get("/tweets");

        foreach var tweet in response {
            json jsonTweet = tweet.toJson();
            string message = jsonTweet.toJsonString();
            check kafkaProducer->send({ topic: TOPIC, value: message.toBytes() });
        }
    }
}
