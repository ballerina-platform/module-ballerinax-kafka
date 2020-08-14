import ballerina/filepath;
const TEST_PATH = "src/kafka/tests/";

function getAbsoluteTestPath(string subdirectoryPath) returns string|error {
    var relativePathResult = filepath:build(TEST_PATH, subdirectoryPath);
    if (relativePathResult is error) {
        return relativePathResult;
    }
    string relativePath = <string>relativePathResult;
    return filepath:absolute(relativePath);
}
