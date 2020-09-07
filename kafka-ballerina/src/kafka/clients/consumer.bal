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

import ballerina/lang.'object;

# Represents a Kafka consumer endpoint.
#
# + consumerConfig - Used to store configurations related to a Kafka connection
public type Consumer client object {
    *'object:Listener;

    public ConsumerConfiguration consumerConfig;
    private string keyDeserializerType;
    private string valueDeserializerType;
    private Deserializer? keyDeserializer = ();
    private Deserializer? valueDeserializer = ();

    # Creates a new Kafka `Consumer`.
    #
    # + config - Configurations related to consumer endpoint
    public function init (ConsumerConfiguration config) {
        self.consumerConfig = config;
        self.keyDeserializerType = config.keyDeserializerType;
        self.valueDeserializerType = config.valueDeserializerType;

        if (self.keyDeserializerType == DES_CUSTOM) {
            var keyDeserializerObject = config?.keyDeserializer;
            if (keyDeserializerObject is ()) {
                panic createConsumerError("Invalid keyDeserializer config: Please Provide a " +
                                        "valid custom deserializer for the keyDeserializer");
            } else {
                self.keyDeserializer = keyDeserializerObject;
            }
        }
        if (self.keyDeserializerType == DES_AVRO) {
            var schemaRegistryUrl = config?.schemaRegistryUrl;
            if (schemaRegistryUrl is ()) {
                panic createConsumerError("Missing schema registry URL for the Avro serializer. Please " +
                            "provide 'schemaRegistryUrl' configuration in 'kafka:ProducerConfiguration'.");
            }
        }

        if (self.valueDeserializerType == DES_CUSTOM) {
            var valueDeserializerObject = config?.valueDeserializer;
            if (valueDeserializerObject is ()) {
                panic createConsumerError("Invalid valueDeserializer config: Please Provide a" +
                                        " valid custom deserializer for the valueDeserializer");
            } else {
                self.valueDeserializer = valueDeserializerObject;
            }
        }
        if (self.valueDeserializerType == DES_AVRO) {
            var schemaRegistryUrl = config?.schemaRegistryUrl;
            if (schemaRegistryUrl is ()) {
                panic createConsumerError("Missing schema registry URL for the Avro deserializer. Please " +
                            "provide 'schemaRegistryUrl' configuration in 'kafka:ConsumerConfiguration'.");
            }
        }
        checkpanic self->connect();

        string[]? topics = config?.topics;
        if (topics is string[]){
            checkpanic self->subscribe(topics);
        }
    }

    # Starts the registered services.
    #
    # + return - An `kafka:ConsumerError` if an error is encountered while starting the server or else nil
    public function __start() returns error? {
        return 'start(self);
    }

    # Stops the kafka listener.
    #
    # + return - An `kafka:ConsumerError` if an error is encountered during the listener stopping process or else nil
    public function __gracefulStop() returns error? {
        return stop(self);
    }

    # Stops the kafka listener.
    #
    # + return - An `kafka:ConsumerError` if an error is encountered during the listener stopping process or else nil
    public function __immediateStop() returns error? {
        return stop(self);
    }

    # Gets called every time a service attaches itself to the listener.
    #
    # + s - The service to be attached
    # + name - Name of the service
    # + return - An `kafka:ConsumerError` if an error is encountered while attaching the service or else nil
    public function __attach(service s, string? name = ()) returns error? {
        return register(self, s, name);
    }

    # Detaches a consumer service from the listener.
    #
    # + s - The service to be detached
    # + return - An `kafka:ConsumerError` if an error is encountered while detaching a service or else nil
    public function __detach(service s) returns error? {
    }

    # Assigns consumer to a set of topic partitions.
    #
    # + partitions - Topic partitions to be assigned
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    public remote function assign(TopicPartition[] partitions) returns ConsumerError? {
        return consumerAssign(self, partitions);
    }

    # Closes the consumer connection of the external Kafka broker.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->close();
    # ```
    #
    # + duration - Timeout duration for the close operation execution
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function close(int duration = -1) returns ConsumerError? {
        return consumerClose(self, duration);
    }

    # Commits the current consumed offsets for the consumer.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->commit();
    # ```
    #
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function 'commit() returns ConsumerError? {
        return consumerCommit(self);
    }

    # Commits given offsets and partitions for the given topics, for consumer.
    #
    # + duration - Timeout duration for the commit operation execution
    # + offsets - Offsets to be commited
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    public remote function commitOffset(PartitionOffset[] offsets, int duration = -1) returns ConsumerError? {
        return consumerCommitOffset(self, offsets, duration);
    }

    # Connects the consumer to the provided host in the consumer configs.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->connect();
    # ```
    #
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function connect() returns ConsumerError? {
        return consumerConnect(self);
    }

    # Retrieves the currently-assigned partitions for the consumer.
    # ```ballerina
    # kafka:TopicPartition[]|kafka:ConsumerError result = consumer->getAssignment();
    # ```
    #
    # + return - Array of assigned partitions for the consumer if executes successfully or else a `kafka:ConsumerError`
    public remote function getAssignment() returns TopicPartition[]|ConsumerError {
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
    public remote function getAvailableTopics(int duration = -1) returns string[]|ConsumerError {
        return consumerGetAvailableTopics(self, duration);
    }

    # Retrieves the start offsets for given set of partitions.
    #
    # + partitions - Array of topic partitions to get the starting offsets
    # + duration - Timeout duration for the get beginning offsets execution
    # + return - Starting offsets for the given partitions if executes successfully or else `kafka:ConsumerError`
    public remote function getBeginningOffsets(TopicPartition[] partitions, int duration = -1)
    returns PartitionOffset[]|ConsumerError {
        return consumerGetBeginningOffsets(self, partitions, duration);
    }

    # Retrieves the last committed offsets for the given topic partitions.
    #
    # + partition - The `TopicPartition` in which the committed offset is returned for consumer
    # + duration - Timeout duration for the get committed offset operation to execute
    # + return - The last committed offset for the consumer for the given partition if there is a committed offset
    #            present, `()` if there are no committed offsets or else a `kafka:ConsumerError`
    public remote function getCommittedOffset(TopicPartition partition, int duration = -1)
    returns PartitionOffset|ConsumerError? {
        return consumerGetCommittedOffset(self, partition, duration);
    }

    # Retrieves the last offsets for given set of partitions.
    #
    # + partitions - Set of partitions to get the last offsets
    # + duration - Timeout duration for the get end offsets operation to execute
    # + return - End offsets for the given partitions if executes successfully or else `kafka:ConsumerError`
    public remote function getEndOffsets(TopicPartition[] partitions, int duration = -1)
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
    public remote function getPausedPartitions() returns TopicPartition[]|ConsumerError {
        return consumerGetPausedPartitions(self);
    }

    # Retrieves the offset of the next record that will be fetched, if a records exists in that position.
    #
    # + partition - The `TopicPartition` in which the position is required
    # + duration - Timeout duration for the get position offset operation to execute
    # + return - Offset which will be fetched next (if a records exists in that offset) or else `kafka:ConsumerError` if
    #            the operation fails
    public remote function getPositionOffset(TopicPartition partition, int duration = -1)
    returns int|ConsumerError {
        return consumerGetPositionOffset(self, partition, duration);
    }

    # Retrieves the set of topics, which are currently subscribed by the consumer.
    # ```ballerina
    # string[]|kafka:ConsumerError result = consumer->getSubscription();
    # ```
    #
    # + return - Array of subscribed topics for the consumer if executes successfully or else a `kafka:ConsumerError`
    public remote function getSubscription() returns string[]|ConsumerError {
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
    public remote function getTopicPartitions(string topic, int duration = -1)
    returns TopicPartition[]|ConsumerError {
        return consumerGetTopicPartitions(self, topic, duration);
    }

    # Pauses retrieving messages from a set of partitions.
    #
    # + partitions - Partitions to pause the retrieval of messages
    # + return - `kafka:ConsumerError` if an error is encountered or else nil
    public remote function pause(TopicPartition[] partitions) returns ConsumerError? {
        return consumerPause(self, partitions);
    }

    # Polls the consumer for the records of an external broker.
    # ```ballerina
    # kafka:ConsumerRecord[]|kafka:ConsumerError result = consumer->poll(1000);
    # ```
    #
    # + timeoutValue - Polling time in milliseconds
    # + return - Array of consumer records if executed successfully or else a `kafka:ConsumerError`
    public remote function poll(int timeoutValue) returns ConsumerRecord[]|ConsumerError {
        return consumerPoll(self, timeoutValue);
    }

    # Resumes consumer retrieving messages from set of partitions which were paused earlier.
    #
    # + partitions - Partitions to resume the retrieval of messages
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    public remote function resume(TopicPartition[] partitions) returns ConsumerError? {
        return consumerResume(self, partitions);
    }

    # Seeks for a given offset in a topic partition.
    #
    # + offset - The `PartitionOffset` to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    public remote function seek(PartitionOffset offset) returns ConsumerError? {
        return consumerSeek(self, offset);
    }

    # Seeks the beginning of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    public remote function seekToBeginning(TopicPartition[] partitions) returns ConsumerError? {
        return consumerSeekToBeginning(self, partitions);
    }

    # Seeks end of the offsets for the given set of topic partitions.
    #
    # + partitions - The set of topic partitions to seek
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    public remote function seekToEnd(TopicPartition[] partitions) returns ConsumerError? {
        return consumerSeekToEnd(self, partitions);
    }

    # Subscribes the consumer to the provided set of topics.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->subscribe(["kafka-topic-1", "kafka-topic-2"]);
    # ```
    #
    # + topics - Array of topics to be subscribed to
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function subscribe(string[] topics) returns ConsumerError? {
        if (self.consumerConfig?.groupId is string) {
            return consumerSubscribe(self, topics);
        } else {
            panic createConsumerError("The groupId of the consumer must be set to subscribe to the topics");
        }
    }

    # Subscribes the consumer to the topics, which match the provided pattern.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->subscribeToPattern("kafka.*");
    # ```
    #
    # + regex - Pattern, which should be matched with the topics to be subscribed to
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function subscribeToPattern(string regex) returns ConsumerError? {
        return consumerSubscribeToPattern(self, regex);
    }

    # Subscribes to the provided set of topics with rebalance listening enabled.
    # This function can be used inside a service, to subscribe to a set of topics, while rebalancing the patition
    # assignment of the consumers.
    #
    # + topics - Array of topics to be subscribed to
    # + onPartitionsRevoked - Function which will be executed if partitions are revoked from this consumer
    # + onPartitionsAssigned - Function which will be executed if partitions are assigned this consumer
    # + return - `kafka:ConsumerError` if an error is encountered or else ()
    public remote function subscribeWithPartitionRebalance(string[] topics,
        function(Consumer consumer, TopicPartition[] partitions) onPartitionsRevoked,
        function(Consumer consumer, TopicPartition[] partitions) onPartitionsAssigned)
    returns ConsumerError? {
        return consumerSubscribeWithPartitionRebalance(self, topics, onPartitionsRevoked, onPartitionsAssigned);
    }

    # Unsubscribes from all the topic subscriptions.
    # ```ballerina
    # kafka:ConsumerError? result = consumer->unsubscribe();
    # ```
    #
    # + return - A `kafka:ConsumerError` if an error is encountered or else '()'
    public remote function unsubscribe() returns ConsumerError? {
        return consumerUnsubscribe(self);
    }
};
