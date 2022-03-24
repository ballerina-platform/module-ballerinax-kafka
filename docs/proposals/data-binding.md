# Proposal: Data binding support for Kafka

_Owners_: @shafreenAnfar @dilanSachi @aashikam     
_Reviewers_: @shafreenAnfar @aashikam  
_Created_: 2022/03/14  
_Issues_: [#2751](https://github.com/ballerina-platform/ballerina-standard-library/issues/2751) [#2783](https://github.com/ballerina-platform/ballerina-standard-library/issues/2783)

## Summary
Data binding helps to access the incoming and outgoing message data in the user's desired type. Similar to the Ballerina HTTP package, subtypes of `json`, `xml` will be the supported types. This proposal discusses ways to provide data binding for both on producer side, and the consumer side.

## Goals
- Improve user experience by adding data binding support for `kafka:Service`, `kafka:Producer` and `Kafka:Consumer`.

## Motivation
As of now, the Ballerina Kafka package does not provide direct data binding for sending and receiving messages. Only `kafka:ProducerRecord[]` and `kafka:ConsumerRecord[]` are the supported data types to send and receive messages which only support `byte[]` as the message value type. Therefore, users have to do data manipulations by themselves. With this new feature, user experience can be improved by introducing data binding to reduce the burden of developers converting byte data to the desired format as discussed in the next section.

## Description
Currently, when sending a message, user has to convert the message to `byte[]`.
```ballerina
type Person record {|
    string name;
    int age;
|};

Person person = {
    age: 10,
    name: "Bob"
};
check producer->send({ topic: TOPIC, value: person.toString().toBytes()});
```
When receiving the same message,
```ballerina
kafka:ConsumerRecord[] records = check consumer->poll(2);
foreach kafka:ConsumerRecord 'record in records {
    string messageContent = check string:fromBytes('record.value);
    Person person = check value:fromJsonStringWithType(messageContent);
}
```
Instead of this, if data binding support is introduced, user can easily send and receive the messages in the desired format.
```ballerina
check producer->send({ topic: TOPIC, value: person});

PersonRecord[] personRecords = check consumer->poll(2);
```

### Listener
`kafka:Listener` receive messages in the `onConsumerRecord` method of the `kafka:Service`.
```ballerina
remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns kafka:Error?;
```
This will be updated to accept the above-mentioned parameter types. The user can state the required data type as a parameter in the remote function signature. So the received data will be converted to the requested type and dispatched to the remote function.
Therefore, following scenarios will be available for the user.
```ballerina
remote function onConsumerRecord(kafka:Caller caller, json|xml[]|byte[] data, kafka:ConsumerRecord[] records) returns kafka:Error?;
```
```ballerina
remote function onConsumerRecord(kafka:Caller caller, json|xml|byte[] data) returns kafka:Error?;
```
### Producer
The `kafka:Producer` client has `send(kafka:ProducerRecord record)` API to send data to the Kafka server. Currently, `kafka:ProducerRecord` is as follows.
```ballerina
# Details related to the producer record.
#
# + topic - Topic to which the record will be appended
# + key - Key that is included in the record
# + value - Record content
# + timestamp - Timestamp of the record, in milliseconds since epoch
# + partition - Partition to which the record should be sent
public type ProducerRecord record {|
    string topic;
    byte[] key?;
    byte[] value;
    int timestamp?;
    int partition?;
|};
```
This will be updated as,
```ballerina
public type ProducerRecord record {|
    string topic;
    byte[] key?;
    json|xml|byte[] value;
    int timestamp?;
    int partition?;
|};
```
Whatever the data type given as the value will be converted to a `byte[]` internally and sent to the Kafka server. If the data binding fails, a `kafka:Error` will be returned from the API.
### Consumer
To consume messages, the consumer client has `poll()` API which returns `kafka:ConsumerRecord[]`.
```ballerina
# Polls the external broker to retrieve messages.
#
# + timeout - Polling time in seconds
# + return - Array of consumer records if executed successfully or else a `kafka:Error`
isolated remote function poll(decimal timeout) returns ConsumerRecord[]|Error;
```
`kafka:ConsumerRecord` will be updated to allow data binding as follows.
```ballerina
public type ConsumerRecord record {|
    byte[] key?;
    json|xml|byte[] value;
    int timestamp;
    PartitionOffset offset;
|};
```
That way, user can receive messages in the desired type via the following way.
```ballerina
public type Person record {|
    string name;
    string age;
|};

public type PersonRecord record {|
    *ConsumerRecord;
    Person value;
|};

PersonRecord[] persons = check consumer->poll(2);
```
To allow these `poll()` function will also be updated to return the desired type,
```ballerina
isolated remote function poll(decimal timeout, typedesc<json|xml[]|kafka:ConsumerRecord[]> T = <>) returns T|Error;
```
For the cases where the user wants none of the `kafka:ConsumerRecord` information, `pollWithType()` API will be introduced.
```ballerina
isolated remote function pollWithType(decimal timeout, typedesc<json|xml[]> T = <>) returns T|Error;
```
With this, user can do the following.
```ballerina
Person[] persons = check consumer->pollWithType(2);
```
With this new data binding improvement, the compiler plugin validation for `onConsumerRecord` function will also be updated to allow types other than `kafka:ConsumerRecord`.
## Testing
- Testing the runtime data type conversions on the producer, consumer & listener.
- Testing compiler plugin validation to accept new data types.
