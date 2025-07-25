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

import ballerina/io;
import ballerina/time;
import ballerinax/kafka;

const string TRADE_TOPIC = "trades";

configurable string kafkaBroker = "localhost:9092";

final kafka:Consumer consumer = check new (kafkaBroker, {
    clientId: "stock-trade-analyzer",
    groupId: "stock-trade-analyzer-group",
    maxPollRecords: 100,
    offsetReset: kafka:OFFSET_RESET_LATEST
});

public function main(string time, string symbol) returns error? {
    int timestamp = check getTimestamp(time);
    kafka:TopicPartitionTimestamp[] timestamps = check getTopicPartitionTimestamps(timestamp);
    kafka:TopicPartitionOffset[] partitionOffsets = check consumer->offsetsForTimes(timestamps);
    foreach kafka:TopicPartitionOffset partitionOffset in partitionOffsets {
        check seekToPartitionOffset(partitionOffset);
    }
    TradeRecord[] records = check consumer->poll(10);
    _ = from TradeRecord tradeRecord in records
        where tradeRecord.value.symbol == symbol
        select io:println(tradeRecord);
}

isolated function getTopicPartitionTimestamps(int timestamp) returns kafka:TopicPartitionTimestamp[]|error {
    kafka:TopicPartition[] partitions = check consumer->getTopicPartitions(TRADE_TOPIC);
    check consumer->assign(partitions);
    kafka:TopicPartitionTimestamp[] timestamps = [];
    foreach kafka:TopicPartition partition in partitions {
        timestamps.push([partition, timestamp]);
    }
    return timestamps;
}

isolated function getTimestamp(string time) returns int|error {
    time:Utc timestamp = check time:utcFromString(time);
    return timestamp[0];
}

isolated function seekToPartitionOffset(kafka:TopicPartitionOffset partitionOffset) returns error? {
    kafka:TopicPartition partition = partitionOffset[0];
    kafka:OffsetAndTimestamp? offsetAndTimestamp = partitionOffset[1];
    if offsetAndTimestamp is () {
        return error(string `No offsets found for topic ${partition.topic} and partition ${partition.partition}`);
    }
    check consumer->seek({
        partition,
        offset: offsetAndTimestamp.offset
    });
}
