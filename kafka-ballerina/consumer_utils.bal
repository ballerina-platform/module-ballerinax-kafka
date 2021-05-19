// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/jballerina.java;

isolated function consumerClose(Consumer consumer, decimal duration) returns Error? =
@java:Method {
    name: "close",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

isolated function consumerConnect(Consumer|Listener consumer) returns Error? =
@java:Method {
    name: "connect",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

isolated function consumerPause(Consumer consumer, TopicPartition[] partitions) returns Error? =
@java:Method {
    name: "pause",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

isolated function consumerResume(Consumer consumer, TopicPartition[] partitions) returns Error? =
@java:Method {
    name: "resume",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

isolated function consumerCommit(Consumer|Caller consumer) returns Error? =
@java:Method {
    name: "commit",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
} external;

isolated function consumerCommitOffset(Consumer|Caller consumer, PartitionOffset[] offsets, decimal duration = -1)
returns Error? =
@java:Method {
    name: "commitOffset",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
} external;

isolated function consumerAssign(Consumer consumer, TopicPartition[] partitions) returns Error? =
@java:Method {
    name: "assign",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetAssignment(Consumer consumer) returns TopicPartition[]|Error =
@java:Method {
    name: "getAssignment",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetAvailableTopics(Consumer consumer, decimal duration) returns string[]|Error =
@java:Method {
    name: "getAvailableTopics",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetPausedPartitions(Consumer consumer) returns TopicPartition[]|Error =
@java:Method {
    name: "getPausedPartitions",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetTopicPartitions(Consumer consumer, string topic, decimal duration = -1)
returns TopicPartition[]|Error =
@java:Method {
    name: "getTopicPartitions",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetSubscription(Consumer consumer) returns string[]|Error =
@java:Method {
    name: "getSubscription",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

isolated function consumerGetBeginningOffsets(Consumer consumer, TopicPartition[] partitions, decimal duration)
returns PartitionOffset[]|Error =
@java:Method {
    name: "getBeginningOffsets",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

isolated function consumerGetCommittedOffset(Consumer consumer, TopicPartition partition, decimal duration)
returns PartitionOffset|Error? =
@java:Method {
    name: "getCommittedOffset",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

isolated function consumerGetEndOffsets(Consumer consumer, TopicPartition[] partitions, decimal duration)
returns PartitionOffset[]|Error =
@java:Method {
    name: "getEndOffsets",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

isolated function consumerGetPositionOffset(Consumer consumer, TopicPartition partition, decimal duration = -1)
returns int|Error =
@java:Method {
    name: "getPositionOffset",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

isolated function consumerPoll(Consumer consumer, decimal timeoutValue) returns ConsumerRecord[]|Error =
@java:Method {
    name: "poll",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Poll"
} external;

isolated function consumerSeek(Consumer consumer, PartitionOffset offset) returns Error? =
@java:Method {
    name: "seek",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

isolated function consumerSeekToBeginning(Consumer consumer, TopicPartition[] partitions) returns Error? =
@java:Method {
    name: "seekToBeginning",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

isolated function consumerSeekToEnd(Consumer consumer, TopicPartition[] partitions) returns Error? =
@java:Method {
    name: "seekToEnd",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

isolated function consumerSubscribe(Consumer|Listener consumer, string[] topics) returns Error? =
@java:Method {
    name: "subscribe",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

isolated function consumerSubscribeWithPattern(Consumer consumer, string regex) returns Error? =
@java:Method {
    name: "subscribeToPattern",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

isolated function consumerUnsubscribe(Consumer consumer) returns Error? =
@java:Method {
    name: "unsubscribe",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

//isolated function register(Listener lis, Service serviceType, string[]|string? name) returns Error? =
//@java:Method {
//    'class: "org.ballerinalang.messaging.kafka.service.Register"
//} external;

//isolated function 'start(Listener lis) returns Error? =
//@java:Method {
//    'class: "org.ballerinalang.messaging.kafka.service.Start"
//} external;

//isolated function stop(Listener lis) returns Error? =
//@java:Method {
//    'class: "org.ballerinalang.messaging.kafka.service.Stop"
//} external;
