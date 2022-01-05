import ballerinax/kafka;
import ballerina/lang.runtime;
import ballerina/http;
import ballerina/time;

const string MESSAGE = "Hello";
const string TOPIC = "perf-topic";
const string KAFKA_CLUSTER = "kafka:9092";
const int MESSAGE_COUNT = 100000;

int errorCount = 0;
int msgCount = 0;
time:Seconds duration = 0;
boolean finished = false;

service /kafka on new http:Listener(9100) {

    resource function get publish() returns string|error? {
        check startListener();
        _ = start publishMessages();
        return "Started publishing messages to Kafka cluster";
    }

    resource function get getResults() returns boolean|map<string> {
        if finished {
            return {
                errorCount: errorCount.toString(),
                time: duration.toString(),
                sentCount: MESSAGE_COUNT.toString(),
                receivedCount: msgCount.toString()
            };
        } else {
            return finished;
        }
    }
}

function publishMessages() returns time:Seconds|error {
    time:Utc startedTime = time:utcNow();
    kafka:Producer producer = check new(KAFKA_CLUSTER);
    foreach int i in 0...MESSAGE_COUNT {
        check producer->send({
            value: MESSAGE.toBytes(),
            topic: TOPIC
        });
    }
    time:Utc endedTime = time:utcNow();
    finished = true;
    return time:utcDiffSeconds(endedTime, startedTime);
}

function startListener() returns error? {
    kafka:ConsumerConfiguration consumerConfigs = {
        groupId: "consumer",
        topics: [TOPIC],
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        pollingInterval: 1
    };
    kafka:Listener kafkaListener = check new (KAFKA_CLUSTER, consumerConfigs);
    check kafkaListener.attach(kafkaService);
    check kafkaListener.start();
    runtime:registerListener(kafkaListener);
}

kafka:Service kafkaService =
service object {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            string|error messageContent = 'string:fromBytes(consumerRecord.value);
            if messageContent !is string || messageContent != MESSAGE {
                errorCount += 1;
            } else {
                msgCount += 1;
            }
        }
    }
};
