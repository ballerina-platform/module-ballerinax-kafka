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
import ballerina/random;
import ballerina/time;

// User-defined tweet record type.
type Tweet record {|
    string created_at;
    int id;
    string text;
|};

service / on new http:Listener(9090) {

    // Responds with an array of tweets to HTTP GET requests.
    resource function get tweets() returns json {

        // Creates an array of tweets.
        json[] jsonResponse = [];
        int i = 0;
        while(i < 20) {
            jsonResponse[i] = generateTweet();
            i += 1;
        }
        return jsonResponse;
    }
}

// Generates a tweet, which has a random integer.
function generateTweet() returns json {
    int id = 1;

    // Generates a random number between 1(inclusive) and 100000(exclusive).
    int|error randomInt = random:createIntInRange(1, 100000);

    if (randomInt is int) {
        id = randomInt;
    } else {
        log:printError("Error occurred while generating a random ID.");
    }

    // Gets the current instant of the system clock (seconds from the epoch of
    // 1970-01-01T00:00:00).
    time:Utc currentUtc = time:utcNow();

    // Converts a given `time:Utc` to a RFC 3339 timestamp.
    string utcString = time:utcToString(currentUtc);
    string text = "This tweet has the random number: " + id.toString();

    Tweet tweet = {
        created_at: utcString,
        id: id,
        text: text
    };
    return tweet.toJson();
}
