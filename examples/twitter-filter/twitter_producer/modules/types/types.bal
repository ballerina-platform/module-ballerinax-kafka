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

// Details related to a tweet.
public type Tweet record {|
    string created_at;
    int id;
    string text;
    User user;
|};

// Details related to a Twitter user account.
public type User record {|
    int id;
    string name;
    string screen_name;
    string location;
    string url;
    string description;
    PublicMetrics public_metrics;
|};

// Public metrics related to a Twitter user account.
public type PublicMetrics record {|
    int followers_count;
    int following_count;
    int tweet_count;
    int listed_count;
|};
