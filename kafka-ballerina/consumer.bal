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

    # Creates a new Kafka `Consumer`.
    #
    # + config - Configurations related to consumer endpoint
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public isolated function init (ConsumerConfiguration config) returns ConsumerError? {
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
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    isolated remote function assign(TopicPartition[] partitions) returns ConsumerError? {
        return consumerAssign(self, partitions);
    }

    # Closes the consumer connection of the external Kafka broker.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->close();
    # ```
    #
    # + duration - Timeout duration for the close operation execution
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    isolated remote function close(int duration = -1) returns ConsumerError? {
        return consumerClose(self, duration);
    }

    # Commits the current consumed offsets for the consumer.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->commit();
    # ```
    #
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    isolated remote function 'commit() returns ConsumerError? {
        return consumerCommit(self);
    }

    # Commits given offsets and partitions for the given topics, for consumer.
    #
    # + duration - Timeout duration for the commit operation execution
    # + offsets - Offsets to be commited
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    isolated remote function commitOffset(PartitionOffset[] offsets, int duration = -1) returns ConsumerError? {
        return consumerCommitOffset(self, offsets, duration);
    }

    # Retrieves the currently-assigned partitions for the consumer.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ConsumerError result = consumer->getAssignment();
    # ```
    #
    # + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:ConsumerError`
    isolated remote function getAssignment() returns TopicPartition[]|ConsumerError {
        return consumerGetAssignment(self);
    }

    # Retrieves the available list of topics for a particular consumer.
    # ```ballerina
    # string[]|kafka:ConsumerError result = consumer->getAvailableTopics();
    # ```
    #
    # + duration - Timeout duration for the execution of the `get available topics` operation
    # + return - Array of topics currently available (authorized) for the consumer to subscribe or else
    #           a `kafka:ConsumerError`
    isolated remote function getAvailableTopics(int duration = -1) returns string[]|ConsumerError {
        return consumerGetAvailableTopics(self, duration);
    }

    # Retrieves the start offsets for given set of partitions.
    #
    # + partitions - Array of topic partitions to get the starting offsets
    # + duration - Timeout duration for the get beginning offsets execution
    # + return - Starting offsets for the given partitions if executes successfully or else `kafka:ConsumerError`
    isolated remote function getBeginningOffsets(TopicPartition[] partitions, int duration = -1)
    returns PartitionOffset[]|ConsumerError {
        return consumerGetBeginningOffsets(self, partitions, duration);
    }

    # Retrieves the last committed offsets for the given topic partitions.
    #
    # + partition - The `TopicPartition` in which the committed offset is returned for consumer
    # + duration - Timeout duration for the get committed offset operation to execute
    # + return - The last committed offset for the consumer for the given partition if there is a committed offset
    #            present, `()` if there are no committed offsets or else a `kafka:ConsumerError`
    isolated remote function getCommittedOffset(TopicPartition partition, int duration = -1)
    returns PartitionOffset|ConsumerError? {
        return consumerGetCommittedOffset(self, partition, duration);
    }

    # Retrieves the last offsets for given set of partitions.
    #
    # + partitions - Set of partitions to get the last offsets
    # + duration - Timeout duration for the get end offsets operation to execute
    # + return - End offsets for the given partitions if executes successfully or else `kafka:ConsumerError`
    isolated remote function getEndOffsets(TopicPartition[] partitions, int duration = -1)
    returns PartitionOffset[]|ConsumerError {
        return consumerGetEndOffsets(self, partitions, duration);
    }

    # Retrieves the partitions, which are currently paused.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ConsumerError result = consumer->getPausedPartitions();
    # ```
    #
    # + return - Set of partitions paused from message retrieval if executes successfully or else
    #            a `kafka:ConsumerError`
    isolated remote function getPausedPartitions() returns TopicPartition[]|ConsumerError {
        return consumerGetPausedPartitions(self);
    }

    # Retrieves the offset of the next record that will be fetched, if a records exists in that position.
    #
    # + partition - The `TopicPartition` in which the position is required
    # + duration - Timeout duration for the get position offset operation to execute
    # + return - Offset which will be fetched next (if a records exists in that offset) or else `kafka:ConsumerError` if
    #            the operation fails
    isolated remote function getPositionOffset(TopicPartition partition, int duration = -1)
    returns int|ConsumerError {
        return consumerGetPositionOffset(self, partition, duration);
    }

    # Retrieves the set of topics, which are currently subscribed by the consumer.
    # ```ballerina
    # string[]|kafka:ConsumerError result = consumer->getSubscription();
    # ```
    #
    # + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:ConsumerError`
    isolated remote function getSubscription() returns string[]|ConsumerError {
        return consumerGetSubscription(self);
    }

    # Retrieves the set of partitions to which the topic belongs.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ConsumerError result = consumer->getTopicPartitions("kafka-topic");
    # ```
    #
    # + topic - The topic for which the partition information is needed
    # + duration - Timeout duration for the `get topic partitions` operation to execute
    # + return - Array of partitions for the given topic if executes successfully or else a `kafka:ConsumerError`
    isolated remote function getTopicPartitions(string topic, int duration = -1)
    returns TopicPartition[]|ConsumerError {
        return consumerGetTopicPartitions(self, topic, duration);
    }

    # Pauses retrieving messages from a set of partitions.
    #
    # + partitions - Partitions to pause the retrieval of messages
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    isolated remote function pause(TopicPartition[] partitions) returns ConsumerError? {
        return consumerPause(self, partitions);
    }

    # Polls the consumer for the records of an external broker.
    # ```ballerina
    # kafka:ConsumerRecord[]|kafka:ConsumerError result = consumer->poll(1000);
    # ```
    #
    # + timeoutValue - Polling time in milliseconds
    # + return - Array of consumer records if executed successfully or else a `kafka:ConsumerError`
    isolated remote function poll(int timeoutValue) returns ConsumerRecord[]|ConsumerError {
        return consumerPoll(self, timeoutValue);
    }

    # Resumes consumer retrieving messages from set of partitions which were paused earlier.
    #
    # + partitions - Partitions to resume the retrieval of messages
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    isolated remote function resume(TopicPartition[] partitions) returns ConsumerError? {
        return consumerResume(self, partitions);
    }

    # Seeks for a given offset in a topic partition.
    #
    # + offset - The `PartitionOffset` to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    isolated remote function seek(PartitionOffset offset) returns ConsumerError? {
        return consumerSeek(self, offset);
    }

    # Seeks the beginning of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    isolated remote function seekToBeginning(TopicPartition[] partitions) returns ConsumerError? {
        return consumerSeekToBeginning(self, partitions);
    }

    # Seeks end of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    isolated remote function seekToEnd(TopicPartition[] partitions) returns ConsumerError? {
        return consumerSeekToEnd(self, partitions);
    }

    # Subscribes the consumer to the provided set of topics.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
    # ```
    #
    # + topics - Array of topics to be subscribed to
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    isolated remote function subscribe(string[] topics) returns ConsumerError? {
        if (self.consumerConfig?.groupId is string) {
            return consumerSubscribe(self, topics);
        } else {
            panic createConsumerError("The groupId of the consumer must be set to subscribe to the topics");
        }
    }

    # Subscribes the consumer to the topics, which match the provided pattern.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->subscribeWithPattern("kafka.*");
    # ```
    #
    # + regex - Pattern, which should be matched with the topics to be subscribed to
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    isolated remote function subscribeWithPattern(string regex) returns ConsumerError? {
        return consumerSubscribeWithPattern(self, regex);
    }

    # Unsubscribes from all the topic subscriptions.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->unsubscribe();
    # ```
    #
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    isolated remote function unsubscribe() returns ConsumerError? {
        return consumerUnsubscribe(self);
    }
}

isolated function connect(Consumer|Listener consumer) returns ConsumerError? {
    return consumerConnect(consumer);
}
