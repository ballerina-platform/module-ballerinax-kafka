// Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

# Represents a Kafka consumer endpoint.
#
# + consumerConfig - Used to store configurations related to a Kafka connection
public client class Consumer {

    public ConsumerConfiguration consumerConfig;
    private string keyDeserializerType;
    private string valueDeserializerType;
    private string|string[] bootstrapServers;

    # Creates a new Kafka `Consumer`.
    #
    # + bootstrapServers - List of remote server endpoints of kafka brokers
    # + config - Configurations related to consumer endpoint
    # + return - A `kafka:Error` if an error is encountered or else '()'
    public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers;
        self.consumerConfig = config;
        self.keyDeserializerType = DES_BYTE_ARRAY;
        self.valueDeserializerType = DES_BYTE_ARRAY;
        check connect(self);

        string[]? topics = config?.topics;
        if (topics is string[]){
            check self->subscribe(topics);
        }
    }

    # Assigns consumer to a set of topic partitions.
    #
    # + partitions - Topic partitions to be assigned
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function assign(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "assign",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Closes the consumer connection of the external Kafka broker.
    # ```ballerina
    # kafka:Error? result = consumer->close();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the close operation execution
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function close(decimal duration = -1) returns Error? =
    @java:Method {
        name: "close",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Commits the current consumed offsets for the consumer.
    # ```ballerina
    # kafka:Error? result = consumer->commit();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function 'commit() returns Error? =
    @java:Method {
        name: "commit",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
    } external;

    # Commits given offsets and partitions for the given topics, for consumer.
    #
    # + duration - Timeout duration (in seconds) for the commit operation execution
    # + offsets - Offsets to be commited
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error? =
    @java:Method {
        name: "commitOffset",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Commit"
    } external;

    # Retrieves the currently-assigned partitions for the consumer.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:Error result = consumer->getAssignment();
    # ```
    #
    # + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getAssignment() returns TopicPartition[]|Error =
    @java:Method {
        name: "getAssignment",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the available list of topics for a particular consumer.
    # ```ballerina
    # string[]|kafka:Error result = consumer->getAvailableTopics();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the execution of the `get available topics` operation
    # + return - Array of topics currently available (authorized) for the consumer to subscribe or else
    #           a `kafka:Error`
    isolated remote function getAvailableTopics(decimal duration = -1) returns string[]|Error =
    @java:Method {
        name: "getAvailableTopics",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the start offsets for given set of partitions.
    #
    # + partitions - Array of topic partitions to get the starting offsets
    # + duration - Timeout duration (in seconds) for the get beginning offsets execution
    # + return - Starting offsets for the given partitions if executes successfully or else `kafka:Error`
    isolated remote function getBeginningOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error =
    @java:Method {
        name: "getBeginningOffsets",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the last committed offsets for the given topic partitions.
    #
    # + partition - The `TopicPartition` in which the committed offset is returned for consumer
    # + duration - Timeout duration (in seconds) for the get committed offset operation to execute
    # + return - The last committed offset for the consumer for the given partition if there is a committed offset
    #            present, `()` if there are no committed offsets or else a `kafka:Error`
    isolated remote function getCommittedOffset(TopicPartition partition, decimal duration = -1)
    returns PartitionOffset|Error? =
    @java:Method {
        name: "getCommittedOffset",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the last offsets for given set of partitions.
    #
    # + partitions - Set of partitions to get the last offsets
    # + duration - Timeout duration (in seconds) for the get end offsets operation to execute
    # + return - End offsets for the given partitions if executes successfully or else `kafka:Error`
    isolated remote function getEndOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error =
    @java:Method {
        name: "getEndOffsets",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the partitions, which are currently paused.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:Error result = consumer->getPausedPartitions();
    # ```
    #
    # + return - Set of partitions paused from message retrieval if executes successfully or else
    #            a `kafka:Error`
    isolated remote function getPausedPartitions() returns TopicPartition[]|Error =
    @java:Method {
        name: "getPausedPartitions",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the offset of the next record that will be fetched, if a records exists in that position.
    #
    # + partition - The `TopicPartition` in which the position is required
    # + duration - Timeout duration (in seconds) for the get position offset operation to execute
    # + return - Offset which will be fetched next (if a records exists in that offset) or else `kafka:Error` if
    #            the operation fails
    isolated remote function getPositionOffset(TopicPartition partition, decimal duration = -1)
    returns int|Error =
    @java:Method {
        name: "getPositionOffset",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the set of topics, which are currently subscribed by the consumer.
    # ```ballerina
    # string[]|kafka:Error result = consumer->getSubscription();
    # ```
    #
    # + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getSubscription() returns string[]|Error =
    @java:Method {
        name: "getSubscription",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the set of partitions to which the topic belongs.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:Error result = consumer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - The topic for which the partition information is needed
    # + duration - Timeout duration (in seconds) for the `get topic partitions` operation to execute
    # + return - Array of partitions for the given topic if executes successfully or else a `kafka:Error`
    isolated remote function getTopicPartitions(string topic, decimal duration = -1)
    returns TopicPartition[]|Error =
    @java:Method {
        name: "getTopicPartitions",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Pauses retrieving messages from a set of partitions.
    #
    # + partitions - Partitions to pause the retrieval of messages
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function pause(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "pause",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Polls the consumer for the records of an external broker.
    # ```ballerina
    # kafka:ConsumerRecord[]|kafka:Error result = consumer->poll(1000);
    # ```
    #
    # + timeout - Polling time in seconds
    # + return - Array of consumer records if executed successfully or else a `kafka:Error`
    isolated remote function poll(decimal timeout) returns ConsumerRecord[]|Error =
    @java:Method {
        name: "poll",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Poll"
    } external;

    # Resumes consumer retrieving messages from set of partitions which were paused earlier.
    #
    # + partitions - Partitions to resume the retrieval of messages
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function resume(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "resume",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Seeks for a given offset in a topic partition.
    #
    # + offset - The `PartitionOffset` to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seek(PartitionOffset offset) returns Error? =
    @java:Method {
        name: "seek",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
    } external;

    # Seeks the beginning of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seekToBeginning(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "seekToBeginning",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
    } external;

    # Seeks end of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seekToEnd(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "seekToEnd",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.Seek"
    } external;

    # Subscribes the consumer to the provided set of topics.
    # ```ballerina
    # kafka:Error? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
    # ```
    #
    # + topics - Array of topics to be subscribed to
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribe(string[] topics) returns Error? =
    @java:Method {
        name: "subscribe",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;

    # Subscribes the consumer to the topics, which match the provided pattern.
    # ```ballerina
    # kafka:Error? result = consumer->subscribeWithPattern("kafka.*");
    # ```
    #
    # + regex - Pattern, which should be matched with the topics to be subscribed to
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribeWithPattern(string regex) returns Error? =
    @java:Method {
        name: "subscribeToPattern",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;

    # Unsubscribes from all the topic subscriptions.
    # ```ballerina
    # kafka:Error? result = consumer->unsubscribe();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function unsubscribe() returns Error? =
    @java:Method {
        name: "unsubscribe",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;
}

//isolated function connect(Consumer|Listener consumer) returns Error? {
//    return consumerConnect(consumer);
//}
