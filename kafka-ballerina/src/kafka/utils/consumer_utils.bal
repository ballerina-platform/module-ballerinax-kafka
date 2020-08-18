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

import ballerina/java;

function consumerClose(Consumer consumer, int duration) returns ConsumerError? =
@java:Method {
    name: "close",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

function consumerConnect(Consumer consumer) returns ConsumerError? =
@java:Method {
    name: "connect",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

function consumerPause(Consumer consumer, TopicPartition[] partitions) returns ConsumerError? =
@java:Method {
    name: "pause",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

function consumerResume(Consumer consumer, TopicPartition[] partitions) returns ConsumerError? =
@java:Method {
    name: "resume",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
} external;

function consumerCommit(Consumer consumer) returns ConsumerError? =
@java:Method {
    name: "commit",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
} external;

function consumerCommitOffset(Consumer consumer, PartitionOffset[] offsets, public int duration = -1)
returns ConsumerError? =
@java:Method {
    name: "commitOffset",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
} external;

function consumerAssign(Consumer consumer, TopicPartition[] partitions) returns ConsumerError? =
@java:Method {
    name: "assign",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetAssignment(Consumer consumer) returns TopicPartition[]|ConsumerError =
@java:Method {
    name: "getAssignment",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetAvailableTopics(Consumer consumer, int duration) returns string[]|ConsumerError =
@java:Method {
    name: "getAvailableTopics",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetPausedPartitions(Consumer consumer) returns TopicPartition[]|ConsumerError =
@java:Method {
    name: "getPausedPartitions",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetTopicPartitions(Consumer consumer, string topic, public int duration = -1)
returns TopicPartition[]|ConsumerError =
@java:Method {
    name: "getTopicPartitions",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetSubscription(Consumer consumer) returns string[]|ConsumerError =
@java:Method {
    name: "getSubscription",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
} external;

function consumerGetBeginningOffsets(Consumer consumer, TopicPartition[] partitions, int duration)
returns PartitionOffset[]|ConsumerError =
@java:Method {
    name: "getBeginningOffsets",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

function consumerGetCommittedOffset(Consumer consumer, TopicPartition partition, int duration)
returns PartitionOffset|ConsumerError? =
@java:Method {
    name: "getCommittedOffset",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

function consumerGetEndOffsets(Consumer consumer, TopicPartition[] partitions, int duration)
returns PartitionOffset[]|ConsumerError =
@java:Method {
    name: "getEndOffsets",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

function consumerGetPositionOffset(Consumer consumer, TopicPartition partition, public int duration = -1)
returns int|ConsumerError =
@java:Method {
    name: "getPositionOffset",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
} external;

function consumerPoll(Consumer consumer, int timeoutValue) returns ConsumerRecord[]|ConsumerError =
@java:Method {
    name: "poll",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Poll"
} external;

function consumerSeek(Consumer consumer, PartitionOffset offset) returns ConsumerError? =
@java:Method {
    name: "seek",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

function consumerSeekToBeginning(Consumer consumer, TopicPartition[] partitions) returns ConsumerError? =
@java:Method {
    name: "seekToBeginning",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

function consumerSeekToEnd(Consumer consumer, TopicPartition[] partitions) returns ConsumerError? =
@java:Method {
    name: "seekToEnd",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
} external;

function consumerSubscribe(Consumer consumer, string[] topics) returns ConsumerError? =
@java:Method {
    name: "subscribe",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

function consumerSubscribeToPattern(Consumer consumer, string regex) returns ConsumerError? =
@java:Method {
    name: "subscribeToPattern",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

function consumerSubscribeWithPartitionRebalance(Consumer consumer, string[] topics,
    function(Consumer consumer, TopicPartition[] partitions) onPartitionsRevoked,
    function(Consumer consumer, TopicPartition[] partitions) onPartitionsAssigned)
returns ConsumerError? =
@java:Method {
    name: "subscribeWithPartitionRebalance",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

function consumerUnsubscribe(Consumer consumer) returns ConsumerError? =
@java:Method {
    name: "unsubscribe",
    class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
} external;

function register(Consumer consumer, service serviceType, string? name) returns ConsumerError? =
@java:Method {
    class: "org.ballerinalang.messaging.kafka.service.Register"
} external;

function 'start(Consumer consumer) returns ConsumerError? =
@java:Method {
    class: "org.ballerinalang.messaging.kafka.service.Start"
} external;

function stop(Consumer consumer) returns ConsumerError? =
@java:Method {
    class: "org.ballerinalang.messaging.kafka.service.Stop"
} external;
