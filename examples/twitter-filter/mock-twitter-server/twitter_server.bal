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
import ballerina/lang.runtime as runtime;
import ballerina/random;
import ballerina/time;

import example/twitterServer.types as types;

@http:ServiceConfig {
    auth: [
        {
            fileUserStoreConfig: {},
            scopes: ["developer"]
        }
    ]
}
service / on new http:Listener(9090) {

    // Responds with an array of tweets to HTTP GET requests.
    resource function get tweets() returns types:Tweet[]|error {

        // Creates an array of tweets.
        types:Tweet[] response = [];
        int i = 0;
        while i < 20 {
            response[i] = check generateTweet();
            i += 1;
        }
        decimal sleepTimer = <decimal>check random:createIntInRange(1, 10);
        runtime:sleep(sleepTimer);
        return response;
    }
}

// Generates a tweet, which has a random integer.
function generateTweet() returns types:Tweet|error {

    // Generates a random number between 1(inclusive) and 100000(exclusive).
    int id = check random:createIntInRange(1, 100000);
    // Generates a random user ID.
    int userId = check random:createIntInRange(1, 1000000);
    // Generates random public metrics details.
    int followersCount = check random:createIntInRange(1, 10000000);
    int followingCount = check random:createIntInRange(1, 10000000);
    int tweetCount = check random:createIntInRange(1, 10000000);
    int listedCount = check random:createIntInRange(1, 10000000);

    // Gets the current instant of the system clock (seconds from the epoch of
    // 1970-01-01T00:00:00).
    time:Utc currentUtc = time:utcNow();

    // Converts a given `time:Utc` to a RFC 3339 timestamp.
    string utcString = time:utcToString(currentUtc);
    string text = "An SQL query went into a bar. He walked up to two tables and said, Hi, can I join you?";

    types:Tweet tweet = {
        created_at: utcString,
        id,
        text,
        user: {
            id: userId,
            name: "Twitter Developer",
            screen_name: "TwitterDev",
            location: "127.0.0.1",
            url: "https://t.co/3ZX3TNiZCY",
            description: "A twitter developer account. #Developer",
            public_metrics: {
                followers_count: followersCount,
                following_count: followingCount,
                tweet_count: tweetCount,
                listed_count: listedCount
            }
        }
    };
    return tweet;
}
