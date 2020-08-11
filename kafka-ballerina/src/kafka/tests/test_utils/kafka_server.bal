import ballerina/java;

public function createKafkaCluster(int zkPort, int brokerPort, string protocol) returns handle|error = @java:Method {
    class: "org.ballerinalang.messaging.kafka.test.utils.TestUtils",
    name: "createKafkaCluster"
} external;

public function stopKafkaCluster(handle kafkaCluster) = @java:Method {
    class: "org.ballerinalang.messaging.kafka.test.utils.TestUtils",
    name: "stopKafkaCluster"
} external;
