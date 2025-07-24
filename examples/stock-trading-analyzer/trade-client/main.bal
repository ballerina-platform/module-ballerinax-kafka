// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
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
import ballerina/io;
import ballerina/lang.runtime;
import ballerina/random;

const string[] SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "META"];

configurable string tradeApiUrl = "http://localhost:9090";

public function main() returns error? {
    foreach int i in 0 ... 10 {
        http:Client httpClient = check new (tradeApiUrl);
        record {} response = check httpClient->post("/trades", {
            symbol: SYMBOLS[check random:createIntInRange(0, 4)],
            price: random:createDecimal(),
            volume: check random:createIntInRange(100, 1000)
        });
        runtime:sleep(2);
    }
}
