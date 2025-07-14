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

import ballerina/lang.runtime;
import ballerina/test;
import ballerinax/kafka;

@test:Config {}
function wordCountCalculatorTest() returns error? {
    kafka:Producer testProducer = check new (kafka:DEFAULT_URL);

    check testProducer->send({topic: INPUT_TOPIC, value: "Test message for kafka topic in kafka examples".toBytes()});
    runtime:sleep(4);

    kafka:ConsumerConfiguration testConsumerConfigs = {
        groupId: "test-word-count-consumer",
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        topics: [OUTPUT_TOPIC]
    };
    kafka:Consumer testConsumer = check new (kafka:DEFAULT_URL, testConsumerConfigs);
    kafka:BytesConsumerRecord[] records = check testConsumer->poll(3);

    test:assertEquals(records.length(), 7);

    map<int> expectedResults = {
        "Test": 1,
        "message": 1,
        "for": 1,
        "kafka": 2,
        "topic": 1,
        "in": 1,
        "examples": 1
    };

    foreach kafka:BytesConsumerRecord 'record in records {
        string countValue = check string:fromBytes('record.value);
        anydata? key = 'record["key"];
        if key is byte[] {
            string word = check string:fromBytes(key);
            int? actualResult = expectedResults[word];
            if actualResult is int {
                test:assertEquals(countValue, actualResult.toString());
            } else {
                test:assertFail("Expected count values in result");
            }
        } else {
            test:assertFail("Expected key values in result");
        }
    }
}
