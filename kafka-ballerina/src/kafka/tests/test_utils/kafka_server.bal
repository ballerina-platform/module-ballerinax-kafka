import ballerina/system;

function createKafkaCluster(string directory, string yamlFilePath) returns error? {
    var result = system:exec("docker-compose", {}, directory, "-f", yamlFilePath, "up", "-d");
    if (result is error) {
        return error("Error occurred while creating the Kafka server");
    }
    system:Process dockerProcess = <system:Process>result;
    var processResult = dockerProcess.waitForExit();
    if (processResult is error) {
        return processResult;
    } else {
        if (processResult != 0) {
            return error("Process exited with non-zero value: " + processResult.toString());
        }
    }
}

function stopKafkaCluster(string directory) returns error? {
    var result = system:exec("docker-compose", {}, directory, "rm", "-svf");
    if (result is error) {
        return result;
    }
    system:Process dockerProcess = <system:Process>result;
    var processResult = dockerProcess.waitForExit();
    if (processResult is error) {
        return processResult;
    } else {
        if (processResult != 0) {
            return error("Process exited with non-zero value: " + processResult.toString());
        }
    }
}
