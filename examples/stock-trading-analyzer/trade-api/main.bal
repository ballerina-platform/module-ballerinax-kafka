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
import ballerinax/kafka;
import ballerina/time;
import ballerina/log;

const string TRADE_TOPIC = "trades";

configurable string kafkaBroker = "localhost:9092";

enum Symbol {
    AAPL,
    GOOG,
    MSFT,
    AMZN,
    META
}

final kafka:Producer producer = check new(kafkaBroker, {
    clientId: "stock-trading-api",
    acks: kafka:ACKS_ALL,
    maxBlock: 6,
    requestTimeout: 2,
    retryCount: 3
});

service on new http:Listener(9090) {
    resource function post trades(Trade trade) returns http:Created|http:BadRequest|http:InternalServerError {
        if !isValidTrade(trade) {
            return <http:BadRequest> {
                body: {
                    message: "Invalid trade symbol"
                }
            };
        }
        time:Utc timestamp = time:utcNow();
        kafka:RecordMetadata|kafka:Error metadata = producer->sendWithMetadata({
            topic: TRADE_TOPIC,
            value: trade,
            key: trade.symbol,
            timestamp: timestamp[0]
        });
        string timestampString = time:utcToString(timestamp);
        if metadata is kafka:Error {
            return <http:InternalServerError> {
                body: {
                    message: "Failed to complete the trade"
                }
            };
        }
        log:printInfo(string `Trade sent to Kafka at ${timestampString}`, metadata = metadata);
        return <http:Created> {
            body: {
                message: "Trade completed successfully"
            }
        };
    }
}

isolated function isValidTrade(Trade trade) returns boolean {
    return trade.symbol is Symbol;
}
