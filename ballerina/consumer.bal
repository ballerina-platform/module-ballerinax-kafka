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
# + consumerConfig - Stores configurations related to a Kafka connection
public client isolated class Consumer {

    final ConsumerConfiguration & readonly consumerConfig;
    private final DeserializerType keyDeserializerType;
    private final DeserializerType valueDeserializerType;
    private final Deserializer? keyDeserializer;
    private final Deserializer? valueDeserializer;
    private final string|string[] & readonly bootstrapServers;

    # Creates a new `kafka:Consumer`.
    #
    # + bootstrapServers - List of remote server endpoints of Kafka brokers
    # + config - Configurations related to the consumer endpoint
    # + return - A `kafka:Error` if an error is encountered or else '()'
    public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers.cloneReadOnly();
        self.consumerConfig = config.cloneReadOnly();
        self.keyDeserializerType = config.keyDeserializerType;
        self.valueDeserializerType = config.valueDeserializerType;
        do {
            self.keyDeserializer = self.keyDeserializerType == DES_AVRO
                ? check new AvroDeserializer(config.schemaRegistryConfig) : ();
            self.valueDeserializer = self.valueDeserializerType == DES_AVRO
                ? check new AvroDeserializer(config.schemaRegistryConfig) : ();
        } on fail error err {
            return error Error("Failed to initialize the deserializers", err);
        }
        check self.consumerInit();

        string|string[]? topics = config?.topics;
        if topics is string|string[] {
            check self->subscribe(topics);
        }
    }

    private isolated function consumerInit() returns Error? =
    @java:Method {
        name: "connect",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    private isolated function consumerSubscribe(string[] topics) returns Error? =
    @java:Method {
        name: "subscribe",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;

    # Assigns consumer to a set of topic partitions.
    # ```ballerina
    # kafka:Error? result = consumer->assign([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Topic partitions to be assigned
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function assign(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "assign",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Closes the consumer connection with the external Kafka broker.
    # ```ballerina
    # kafka:Error? result = consumer->close();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the close operation execution
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function close(decimal duration = -1) returns Error? =
    @java:Method {
        name: "close",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Commits the currently consumed offsets of the consumer.
    # ```ballerina
    # kafka:Error? result = consumer->commit();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function 'commit() returns Error? =
    @java:Method {
        name: "commit",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Commit"
    } external;

    # Commits the given offsets of the specific topic partitions for the consumer.
    # ```ballerina
    # kafka:Error? result = consumer->commitOffset([partitionOffset1, partitionOffset2]);
    # ```
    #
    # + offsets - Offsets to be committed
    # + duration - Timeout duration (in seconds) for the commit operation execution
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error? =
    @java:Method {
        name: "commitOffset",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Commit"
    } external;

    # Retrieves the currently-assigned partitions of the consumer.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getAssignment();
    # ```
    #
    # + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getAssignment() returns TopicPartition[]|Error =
    @java:Method {
        name: "getAssignment",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the available list of topics for a particular consumer.
    # ```ballerina
    # string[] result = check consumer->getAvailableTopics();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the execution of the `getAvailableTopics` operation
    # + return - Array of topics currently available (authorized) for the consumer to subscribe or else
    #            a `kafka:Error`
    isolated remote function getAvailableTopics(decimal duration = -1) returns string[]|Error =
    @java:Method {
        name: "getAvailableTopics",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the start offsets for a given set of partitions.
    # ```ballerina
    # kafka:PartitionOffset[] result = check consumer->getBeginningOffsets([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Array of topic partitions to get the starting offsets
    # + duration - Timeout duration (in seconds) for the `getBeginningOffsets` execution
    # + return - Starting offsets for the given partitions if executes successfully or else a `kafka:Error`
    isolated remote function getBeginningOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error =
    @java:Method {
        name: "getBeginningOffsets",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the lastly committed offset for the given topic partition.
    # ```ballerina
    # kafka:PartitionOffset? result = check consumer->getCommittedOffset(topicPartition);
    # ```
    #
    # + partition - The `TopicPartition` in which the committed offset is returned to the consumer
    # + duration - Timeout duration (in seconds) for the `getCommittedOffset` operation to execute
    # + return - The last committed offset for a given partition for the consumer if there is a committed offset
    #            present, `()` if there are no committed offsets, or else a `kafka:Error`
    isolated remote function getCommittedOffset(TopicPartition partition, decimal duration = -1)
    returns PartitionOffset|Error? =
    @java:Method {
        name: "getCommittedOffset",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the last offsets for a given set of partitions.
    # ```ballerina
    # kafka:PartitionOffset[] result = check consumer->getEndOffsets([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Set of partitions to get the last offsets
    # + duration - Timeout duration (in seconds) for the `getEndOffsets` operation to execute
    # + return - End offsets for the given partitions if executes successfully or else a `kafka:Error`
    isolated remote function getEndOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error =
    @java:Method {
        name: "getEndOffsets",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the partitions, which are currently paused.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getPausedPartitions();
    # ```
    #
    # + return - The set of partitions paused from message retrieval if executes successfully or else a `kafka:Error`
    isolated remote function getPausedPartitions() returns TopicPartition[]|Error =
    @java:Method {
        name: "getPausedPartitions",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the offset of the next record that will be fetched if a record exists in that position.
    # ```ballerina
    # int result = check consumer->getPositionOffset(topicPartition);
    # ```
    #
    # + partition - The `TopicPartition` in which the position is required
    # + duration - Timeout duration (in seconds) for the get position offset operation to execute
    # + return - Offset, which will be fetched next (if a record exists in that offset) or else a `kafka:Error` if
    #            the operation fails
    isolated remote function getPositionOffset(TopicPartition partition, decimal duration = -1)
    returns int|Error =
    @java:Method {
        name: "getPositionOffset",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Retrieves the set of topics, which are currently subscribed by the consumer.
    # ```ballerina
    # string[] result = check consumer->getSubscription();
    # ```
    #
    # + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getSubscription() returns string[]|Error =
    @java:Method {
        name: "getSubscription",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Retrieves the set of partitions to which the topic belongs.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - The topic for which the partition information is needed
    # + duration - Timeout duration (in seconds) for the `getTopicPartitions` operation to execute
    # + return - Array of partitions for the given topic if executes successfully or else a `kafka:Error`
    isolated remote function getTopicPartitions(string topic, decimal duration = -1)
    returns TopicPartition[]|Error =
    @java:Method {
        name: "getTopicPartitions",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.ConsumerInformationHandler"
    } external;

    # Pauses retrieving messages from a set of partitions.
    # ```ballerina
    # kafka:Error? result = consumer->pause([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Set of topic partitions to pause the retrieval of messages
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function pause(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "pause",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Polls the external broker to retrieve messages.
    # ```ballerina
    # kafka:AnydataConsumerRecord[] result = check consumer->poll(10);
    # ```
    #
    # + timeout - Polling time in seconds
    # + T - Optional type description of the required data type
    # + return - Array of consumer records if executed successfully or else a `kafka:Error`
    isolated remote function poll(decimal timeout, typedesc<AnydataConsumerRecord[]> T = <>) returns T|Error =
    @java:Method {
        name: "poll",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Poll"
    } external;

    # Polls the external broker to retrieve messages in the required data type without the `kafka:AnydataConsumerRecord`
    # information.
    # ```ballerina
    # Person[] persons = check consumer->pollPayload(10);
    # ```
    #
    # + timeout - Polling time in seconds
    # + T - Optional type description of the required data type
    # + return - Array of data in the required format if executed successfully or else a `kafka:Error`
    isolated remote function pollPayload(decimal timeout, typedesc<anydata[]> T = <>) returns T|Error =
    @java:Method {
        name: "pollPayload",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Poll"
    } external;

    # Resumes retrieving messages from a set of partitions, which were paused earlier.
    # ```ballerina
    # kafka:Error? result = consumer->resume([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Topic partitions to resume the retrieval of messages
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function resume(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "resume",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Retrieves the offsets for the given topic partitions and timestamps.
    # ```ballerina
    # kafka:TopicPartitionOffset[] result = check consumer->offsetsForTimes([[topicPartition1, timestamp1], [topicPartition2, timestamp2]]);
    # ```
    #
    # + topicPartitionTimestamps - Array of topic partitions with required timestamps
    # + duration - Timeout duration (in seconds) for the `offsetsForTimes` operation to execute
    # + return - Array of topic partition offsets if executed successfully or else a `kafka:Error`
    isolated remote function offsetsForTimes(TopicPartitionTimestamp[] topicPartitionTimestamps, decimal? duration = ())
    returns TopicPartitionOffset[]|Error = @java:Method {
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.GetOffsets"
    } external;

    # Seeks for a given offset in a topic partition.
    # ```ballerina
    # kafka:Error? result = consumer->seek(partitionOffset);
    # ```
    #
    # + offset - The `PartitionOffset` to seek
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function seek(PartitionOffset offset) returns Error? =
    @java:Method {
        name: "seek",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Seek"
    } external;

    # Seeks to the beginning of the offsets for a given set of topic partitions.
    # ```ballerina
    # kafka:Error? result = consumer->seekToBeginning([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - The set of topic partitions to seek
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function seekToBeginning(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "seekToBeginning",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Seek"
    } external;

    # Seeks to the end of the offsets for a given set of topic partitions.
    # ```ballerina
    # kafka:Error? result = consumer->seekToEnd([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - The set of topic partitions to seek
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function seekToEnd(TopicPartition[] partitions) returns Error? =
    @java:Method {
        name: "seekToEnd",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Seek"
    } external;

    # Subscribes the consumer to the provided set of topics.
    # ```ballerina
    # kafka:Error? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
    # ```
    #
    # + topics - The topic/array of topics to subscribe
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribe(string|string[] topics) returns Error? {
        if self.consumerConfig?.groupId is string {
            if topics is string {
                return self.consumerSubscribe([topics]);
            }
            return self.consumerSubscribe(topics);
        } else {
            panic createError("The groupId of the consumer must be set to subscribe to the topics");
        }
    }

    # Subscribes the consumer to the topics, which match the provided pattern.
    # ```ballerina
    # kafka:Error? result = consumer->subscribeWithPattern("kafka.*");
    # ```
    #
    # + regex - The pattern, which should be matched with the topics to be subscribed
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribeWithPattern(string regex) returns Error? =
    @java:Method {
        name: "subscribeToPattern",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;

    # Unsubscribes from all the topics that the consumer is subscribed to.
    # ```ballerina
    # kafka:Error? result = consumer->unsubscribe();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function unsubscribe() returns Error? =
    @java:Method {
        name: "unsubscribe",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;
}
