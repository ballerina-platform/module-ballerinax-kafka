import ballerina/system;
import ballerina/runtime;

function createKafkaCluster() returns error? {
    var createResult = system:exec("docker-compose", {}, "/", "up", "-d");
    if (createResult is error) {
        return error("Error occurred while cleaning / creating the Kafka server");
    }
    runtime:sleep(5000);
}

function stopKafkaCluster() returns error? {
    var cleanResult = system:exec("docker-compose", {}, "/", "rm", "-svf");
    if (cleanResult is error) {
        return cleanResult;
    }
}
