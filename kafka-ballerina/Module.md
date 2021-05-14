# Module Overview

This module provides an implementation to interact with Kafka Brokers via Kafka Consumer and Kafka Producer clients.

Apache Kafka is an open-source distributed event streaming platform used for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

This module supports Kafka 1.x.x and 2.0.0 versions.
## Kafka Producer
A Kafka producer is a Kafka client that publishes records to the Kafka cluster. The producer is thread-safe and sharing a single producer instance across threads will generally be faster than having multiple instances. When working with a Kafka producer, the first thing to do is initialize the producer.
For the producer to execute successfully, an active Kafka broker should be available.

### Initialize the Kafka message producer
The code snippet given below initializes a producer with basic configuration.
```ballerina
import ballerinax/kafka;

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfiguration);
```

### Use the Kafka producer to send messages
To send a message to the Kafka cluster, a `topic` and a `value` must be specified and the `key` is optional.
```ballerina
string message = "Hello World, Ballerina";
string key = "my-key";
check kafkaProducer->send({ topic: "test-kafka-topic", key: key.toBytes(), value: message.toBytes() });
```

### Flush the records sent to the producer
Use the `flush` function to flush the batch of records already sent to the broker by the producer.
```ballerina
check kafkaProducer->'flush();
```

### Retrieve topic partition information of a particular topic
Use the below code snippet to retrieve topic partition information of a topic. A `TopicPartition` is a record containing the `topic` and the index of the partition.
```ballerina
string topic = "kafka-topic";
kafka:TopicPartition[] result = check producer->getTopicPartitions(topic);
```

### Close the producer connection with Kafka cluster
Use the `close` function to close the producer connection to the external Kafka cluster.
```ballerina
check producer->close();
```
## Kafka Consumer
A Kafka consumer is a subscriber responsible for reading records from one or more topics and one or more partitions of a topic. When working with a Kafka producer, the first thing to do is initialize the producer.
For the consumer to execute successfully, an active Kafka broker should be available.

### Initialize the Kafka message consumer
The code snippet given below initializes a consumer with basic configuration.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",    // Unique string that identifies the consumer
    offsetReset: "earliest",    // Offset reset strategy if no initial offset
    topics: ["kafka-topic"]
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfiguration);
```

### Use the Kafka consumer to read messages
You can use the `poll` function to retrieve the messages from a subscribed topic. This uses the builtin byte array deserializer for both the key and the value, which is the default deserializer in the kafka:Consumer.
Following code snippet shows how to retrieve the messages.
```ballerina
// pass a timeout value to the poll function
kafka:ConsumerRecord[] records = check kafkaConsumer->poll(1);

foreach var kafkaRecord in records {
    byte[] messageContent = kafkaRecord.value;
    // try to generate the string value from the byte values
    string result = check string:fromBytes(messageContent);
    io:println("The result is : ", result);
}
```

### View the available topics in the Kafka cluster
You can use the consumer to view the available topics in the cluster using the `getAvailableTopics` function.
```ballerina
string[]|kafka:Error result = kafkaConsumer->getAvailableTopics();
if result is error {
    io:println("Retrieve failed.");
} else {
    // else loop through the topic partitions
    foreach topic in result {
        io:println("Topic : ", topic);
    }
}
```

### Subscribe the consumer to a set of topics
A consumer can subscribe to a topic in the initialization time or later by using the `subscribe` function.

You can define the topic list in the consumer configuration to subscribe to the topics in the initialization time.
```ballerina
string[] topicList = ["kafka-topic-1", "kafka-topic-2", "kafka-topic-3"];

kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",
    offsetReset: "earliest",
    topics: topicList
};
```
Or, you can explicitly subscribe to a list of topics at a later time.
```ballerina
check kafkaConsumer->subscribe(topicList);
```
Consumer can also subscribe to a set of topics with the same pattern.
```ballerina
// provide the regex which should be matched with the topics to be subscribed to
check kafkaConsumer->subscribeWithPattern("kafka-*");
```

### Unsubscribe from all the topics
A consumer can unsubscribe from all the topics subscribed by the consumer.
```ballerina
check kafkaConsumer->unsubscribe();
```

### Assign a consumer to a desired topic partition
The following code snippet shows how to assign a consumer to a set of topic partitions.
```ballerina
// create a topic partition
kafka:TopicPartition topicPartition = {
    topic: "kafka-topic-1",
    partition: 1
};
// pass the topic partitions to the assign function as an array
check kafkaConsumer->assign([topicPartition]);
```

### Retrieve the list of assigned topic partitions
You can get the list of assigned topic partitions of a consumer using the `getAssignment` function.
```ballerina
kafka:TopicPartition[]|kafka:Error result = consumer->getAssignment();
// check if the result is error
if result is kafka:Error {
    io:println("Retrieve failed.");
} else {
    // else loop through the topic partitions
    foreach topicPartition in result {
        io:println("Topic : ", topicPartition.topic, " Partition : ", topicPartition.partition.toString());
    }
}
```

### Pause receiving messages from a set of partitions
Consumer can pause receiving messages from a specific set of partitions using the `pause` function.
```ballerina
kafka:TopicPartition topicPartition = {
    topic: "kafka-topic-2",
    partition: 1
}

check kafkaConsumer->pause([topicPartition]);
```

### Resume receiving messages from paused partitions
Consumer can resume the message receiving from partitions which were paused earlier.
```ballerina
check kafkaConsumer->resume([topicPartition]);
```

### Offset commit
#### Commit the current offsets
The current offset is a pointer to the last record that Kafka has already sent to the consumer in the most recent poll. By committing the offset, it is marked as consumed by the consumer.
```ballerina
check kafkaConsumer->`commit();
```
#### Commit a specific offset
Rather than committing the current offset, a consumer can commit a specific offset if needed.
You can use the function `commitOffset` and pass in `PartitionOffset`s and optional `duration` to the function.
```ballerina
// define a partition offset by providing the specific topic partition and the required offset to commit
kafka:PartitionOffset partitionOffset = {
    partition: topicPartition,
    offset: 10
}

check kafkaConsumer->commitOffset(partitionOffset, 10);
```

### Retrieve offsets
#### Retrieve start offsets of a set of partitions
Following code snippet show how to retrieve the start offsets for a given set of partitions.
```ballerina
PartitionOffset[]|Error result = kafkaConsumer->getBeginningOffsets([topicPartition]);

if result is kafka:Error {
    io:println("Retrieve failed.");
} else {
    // else loop through the partition offsets
    foreach partitionOffset in result {
        io:println("Topic : ", partitionOffset.partition.topic, " Partition : ", partitionOffset.partition.partition.toString(), " Offset : ", partitionOffset.offset.toString());
    }
}
```

#### Retrieve committed offsets of a set of partitions
Following code snippet retrieves the last committed offsets for a given topic partition.
```ballerina
PartitionOffset|Error result = kafkaConsumer->getCommittedOffset(topicPartition);
```

#### Retrieve end offsets of a set of partitions
Following code snippet retrieves the last offsets for a given set of partitions.
```ballerina
PartitionOffset[]|Error result = kafkaConsumer->getEndOffsets([topicPartition]);
```

### Seek the consumer
Seeking allows the consumer to specify a position in the partition and retrieve messages from that position.
#### Seek consumer to the beginning
Consumer can seek to the beginning of the offset for a given set of partitions.
```ballerina
check kafkaConsumer->seekToBeginning([topicPartition]);
```
#### Seek consumer to the end
Consumer can seek to the end of the offset for a given set of partitions.
```ballerina
check kafkaConsumer->seekToEnd([topicPartition]);
```
#### Seek consumer to a specific position
Consumer can seek to a specific offset of a partition by providing a `PartitionOffset`.
```ballerina
kafka:PartitionOffset partitionOffset = {
    partition: topicPartition,
    offset: 10
}

check kafkaConsumer->seekToEnd(partitionOffset);
```

### Close the consumer
You need to close the consumer connection with the external Kafka broker at the end.
```ballerina
check kafkaConsumer->close();
```
## Kafka Listener
The Kafka consumer can be used as a listener to a set of topics without the need to manually `poll` the messages.

You can use the `Caller` to manually commit the offsets of the messages that are read by the service. Following code snippet shows how to initialize, define the listener and how to commit the offsets manually.
```ballerina
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "group-id",
    topics: ["kafka-topic-1"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfiguration);

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) {
        // process the records
        
        // commit the offsets manually
        kafka:Error? commitResult = caller->commit();

        if (commitResult is error) {
            io:println("Error occurred while committing the offsets for the consumer ", 'error = commitResult);
        }
    }
}
```
