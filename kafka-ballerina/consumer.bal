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
    # + bootstrapServers - List of remote server endpoints of Kafka brokers
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
    isolated remote function assign(TopicPartition[] partitions) returns Error? {
        return consumerAssign(self, partitions);
    }

    # Closes the consumer connection with the external Kafka broker.
    # ```ballerina
    # kafka:Error? result = consumer->close();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the close operation execution
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function close(decimal duration = -1) returns Error? {
        return consumerClose(self, duration);
    }

    # Commits the current consumed offsets of the consumer.
    # ```ballerina
    # kafka:Error? result = consumer->commit();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function 'commit() returns Error? {
        return consumerCommit(self);
    }

    # Commits given offsets of specific topic partitions for the consumer.
    # ```ballerina
    # kafka:Error? result = consumer->commitOffset([partitionOffset1, partitionOffset2]);
    # ```
    #
    # + offsets - Offsets to be commited
    # + duration - Timeout duration (in seconds) for the commit operation execution
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error? {
        return consumerCommitOffset(self, offsets, duration);
    }

    # Retrieves the currently-assigned partitions of the consumer.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getAssignment();
    # ```
    #
    # + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getAssignment() returns TopicPartition[]|Error {
        return consumerGetAssignment(self);
    }

    # Retrieves the available list of topics for a particular consumer.
    # ```ballerina
    # string[] result = check consumer->getAvailableTopics();
    # ```
    #
    # + duration - Timeout duration (in seconds) for the execution of the `getAvailableTopics` operation
    # + return - Array of topics currently available (authorized) for the consumer to subscribe or else
    #            a `kafka:Error`
    isolated remote function getAvailableTopics(decimal duration = -1) returns string[]|Error {
        return consumerGetAvailableTopics(self, duration);
    }

    # Retrieves the start offsets for a given set of partitions.
    # ```ballerina
    # kafka:PartitionOffset[] result = check consumer->getBeginningOffsets([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Array of topic partitions to get the starting offsets
    # + duration - Timeout duration (in seconds) for the `getBeginningOffsets` execution
    # + return - Starting offsets for the given partitions if executes successfully or else `kafka:Error`
    isolated remote function getBeginningOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error {
        return consumerGetBeginningOffsets(self, partitions, duration);
    }

    # Retrieves the last committed offset for the given topic partition.
    # ```ballerina
    # kafka:PartitionOffset result = check consumer->getCommittedOffset(topicPartition);
    # ```
    #
    # + partition - The `TopicPartition` in which the committed offset is returned to the consumer
    # + duration - Timeout duration (in seconds) for the `getCommittedOffset` operation to execute
    # + return - The last committed offset for a given partition for the consumer if there is a committed offset
    #            present, `()` if there are no committed offsets or else a `kafka:Error`
    isolated remote function getCommittedOffset(TopicPartition partition, decimal duration = -1)
    returns PartitionOffset|Error? {
        return consumerGetCommittedOffset(self, partition, duration);
    }

    # Retrieves the last offsets for a given set of partitions.
    # ```ballerina
    # kafka:PartitionOffset[] result = check consumer->getEndOffsets([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Set of partitions to get the last offsets
    # + duration - Timeout duration (in seconds) for the `getEndOffsets` operation to execute
    # + return - End offsets for the given partitions if executes successfully or else `kafka:Error`
    isolated remote function getEndOffsets(TopicPartition[] partitions, decimal duration = -1)
    returns PartitionOffset[]|Error {
        return consumerGetEndOffsets(self, partitions, duration);
    }

    # Retrieves the partitions which are currently paused.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getPausedPartitions();
    # ```
    #
    # + return - The set of partitions paused from message retrieval if executes successfully or else a `kafka:Error`
    isolated remote function getPausedPartitions() returns TopicPartition[]|Error {
        return consumerGetPausedPartitions(self);
    }

    # Retrieves the offset of the next record that will be fetched, if a records exists in that position.
    # ```ballerina
    # int result = check consumer->getPositionOffset(topicPartition);
    # ```
    #
    # + partition - The `TopicPartition` in which the position is required
    # + duration - Timeout duration (in seconds) for the get position offset operation to execute
    # + return - Offset which will be fetched next (if a record exists in that offset) or else `kafka:Error` if
    #            the operation fails
    isolated remote function getPositionOffset(TopicPartition partition, decimal duration = -1)
    returns int|Error {
        return consumerGetPositionOffset(self, partition, duration);
    }

    # Retrieves the set of topics which are currently subscribed by the consumer.
    # ```ballerina
    # string[] result = check consumer->getSubscription();
    # ```
    #
    # + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:Error`
    isolated remote function getSubscription() returns string[]|Error {
        return consumerGetSubscription(self);
    }

    # Retrieves the set of partitions to which the topic belongs.
    # ```ballerina
    # kafka:TopicPartition[] result = check consumer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - The topic for which the partition information is needed
    # + duration - Timeout duration (in seconds) for the `getTopicPartitions` operation to execute
    # + return - Array of partitions for the given topic if executes successfully or else a `kafka:Error`
    isolated remote function getTopicPartitions(string topic, decimal duration = -1)
    returns TopicPartition[]|Error {
        return consumerGetTopicPartitions(self, topic, duration);
    }

    # Pauses retrieving messages from a set of partitions.
    # ```ballerina
    # kafka:Error? result = consumer->pause([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Set of topic partitions to pause the retrieval of messages
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function pause(TopicPartition[] partitions) returns Error? {
        return consumerPause(self, partitions);
    }

    # Polls the external broker to retrieve messages.
    # ```ballerina
    # kafka:ConsumerRecord[] result = check consumer->poll(1000);
    # ```
    #
    # + timeout - Polling time in seconds
    # + return - Array of consumer records if executed successfully or else a `kafka:Error`
    isolated remote function poll(decimal timeout) returns ConsumerRecord[]|Error {
        return consumerPoll(self, timeout);
    }

    # Resumes retrieving messages from a set of partitions which were paused earlier.
    # ```ballerina
    # kafka:Error? result = consumer->resume([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - Topic partitions to resume the retrieval of messages
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function resume(TopicPartition[] partitions) returns Error? {
        return consumerResume(self, partitions);
    }

    # Seeks for a given offset in a topic partition.
    # ```ballerina
    # kafka:Error? result = consumer->seek(partitionOffset);
    # ```
    #
    # + offset - The `PartitionOffset` to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seek(PartitionOffset offset) returns Error? {
        return consumerSeek(self, offset);
    }

    # Seeks to the beginning of the offsets for a given set of topic partitions.
    # ```ballerina
    # kafka:Error? result = consumer->seekToBeginning([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seekToBeginning(TopicPartition[] partitions) returns Error? {
        return consumerSeekToBeginning(self, partitions);
    }

    # Seeks to the end of the offsets for a given set of topic partitions.
    # ```ballerina
    # kafka:Error? result = consumer->seekToEnd([topicPartition1, topicPartition2]);
    # ```
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:Error` if an error is encountered or else ()
    isolated remote function seekToEnd(TopicPartition[] partitions) returns Error? {
        return consumerSeekToEnd(self, partitions);
    }

    # Subscribes the consumer to the provided set of topics.
    # ```ballerina
    # kafka:Error? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
    # ```
    #
    # + topics - The array of topics to be subscribed to
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribe(string[] topics) returns Error? {
        if (self.consumerConfig?.groupId is string) {
            return consumerSubscribe(self, topics);
        } else {
            panic createError("The groupId of the consumer must be set to subscribe to the topics");
        }
    }

    # Subscribes the consumer to the topics, which matches the provided pattern.
    # ```ballerina
    # kafka:Error? result = consumer->subscribeWithPattern("kafka.*");
    # ```
    #
    # + regex - The pattern, which should be matched with the topics to be subscribed to
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function subscribeWithPattern(string regex) returns Error? {
        return consumerSubscribeWithPattern(self, regex);
    }

    # Unsubscribes from all the topics that the consumer is subscribed.
    # ```ballerina
    # kafka:Error? result = consumer->unsubscribe();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function unsubscribe() returns Error? {
        return consumerUnsubscribe(self);
    }
}

isolated function connect(Consumer|Listener consumer) returns Error? {
    return consumerConnect(consumer);
}
