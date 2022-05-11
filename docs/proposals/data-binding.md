# Proposal: Data binding support for Kafka

_Owners_: @shafreenAnfar @dilanSachi @aashikam     
_Reviewers_: @shafreenAnfar @aashikam  
_Created_: 2022/03/14  
_Issues_: [#2751](https://github.com/ballerina-platform/ballerina-standard-library/issues/2751) [#2783](https://github.com/ballerina-platform/ballerina-standard-library/issues/2783) [#2880](https://github.com/ballerina-platform/ballerina-standard-library/issues/2880)

## Summary
Data binding helps to access the incoming and outgoing message data in the user's desired type. Similar to the Ballerina HTTP package, subtypes of `anydata` will be the supported types. This proposal discusses ways to provide data binding for both on producer side, and the consumer side.

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
Instead of this, if data binding support is introduced, users can easily send and receive the messages in the desired format.

For this purpose, we will introduce 2 new records for consuming and producing.
```ballerina
# Type related to anydata consumer record.
#
# + key - Key that is included in the record
# + value - Anydata record content
# + timestamp - Timestamp of the record, in milliseconds since epoch
# + offset - Topic partition position in which the consumed record is stored
public type AnydataConsumerRecord record {|
    anydata key?;
    anydata value;
    int timestamp;
    PartitionOffset offset;
|};
```
```ballerina
# Details related to the anydata producer record.
#
# + topic - Topic to which the record will be appended
# + key - Key that is included in the record
# + value - Anydata record content
# + timestamp - Timestamp of the record, in milliseconds since epoch
# + partition - Partition to which the record should be sent
public type AnydataProducerRecord record {|
    string topic;
    anydata key?;
    anydata value;
    int timestamp?;
    int partition?;
|};
```
With these, user can create user-defined subtypes of the above records to achieve data binding as shown below.
```ballerina
public type PersonProducerRecord record {|
    *kafka:AnydataProducerRecord;
    string key?;
    Person value;
|};
check producer->send(personRecord);

public type PersonConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    string key?;
    Person value;
|};
PersonConsumerRecord[] personRecords = check consumer->poll(2);
```
### Listener
`kafka:Listener` receive messages in the `onConsumerRecord` method of the `kafka:Service`.
```ballerina
remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns kafka:Error?;
```
This will be updated to accept the above-mentioned parameter types. User can create a subtype of `kafka:AnydataConsumerRecord` and use it in the function signature or directly use a subtype of `anydata` to get the payload binded directly.
Therefore, following scenarios will be available for the user.
```ballerina
remote function onConsumerRecord(kafka:Caller caller, anydata[] data, kafka:AnydataConsumerRecord[] records) returns kafka:Error?;
```
```ballerina
remote function onConsumerRecord(kafka:Caller caller, anydata[] data) returns kafka:Error?;
```
```ballerina
remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] consumerRecords) returns kafka:Error?;
```
As an example,
```ballerina
remote function onConsumerRecord(kafka:Caller caller, PersonConsumerRecord[] data) returns kafka:Error?;
remote function onConsumerRecord(kafka:Caller caller, Person[] data) returns kafka:Error?;
```
In a case where the user defined payload type is structurally same as `kafka:AnydataConsumerRecord`, user can include the `@Payload` annotation to remove the ambiguity.
```ballerina
public type RandomPayload record {|
    byte[] key?;
    byte[] value;
    int timestamp;
    record {
        record {
            string topic;
            int partition;
        } partition;
        int offset;
    } offset;
|};
remote function onConsumerRecord(kafka:AnydataConsumerRecord[] consumerRecords, @kafka:Payload RandomPayload[] payload) returns kafka:Error?;
```
### Producer
The `kafka:Producer` client has `send(kafka:ProducerRecord record)` API to send data to the Kafka server.
```ballerina
isolated remote function send(kafka:ProducerRecord producerRecord) returns Error?;
```
This will be updated as,
```ballerina
# Produces records to the Kafka server.
#
# + producerRecord - Record to be produced
# + return - A `kafka:Error` if send action fails to send data or else '()'
remote function send(kafka:AnydataProducerRecord producerRecord) returns Error?;
``` 
With this, whatever the data type given as the value will be converted to a `byte[]` internally and sent to the Kafka server. If the data binding fails, a `kafka:Error` will be returned from the API.
### Consumer
To consume messages, the consumer client has `poll()` API which returns `kafka:ConsumerRecord[]`.
```ballerina
remote function poll(decimal timeout) returns ConsumerRecord[]|Error;
```
This will be updated as follows.
```ballerina
# Polls the external broker to retrieve messages.
#
# + timeout - Polling time in seconds
# + T - Optional type description of the required data type
# + return - Array of consumer records if executed successfully or else a `kafka:Error`
isolated remote function poll(decimal timeout, typedesc<AnydataConsumerRecord[]> T = <>) returns T|Error;
```

For the cases where the user wants none of the `kafka:ConsumerRecord` information, `pollPayload()` API will be introduced.
```ballerina
isolated remote function pollPayload(decimal timeout, typedesc<anydata[]> T = <>) returns T|Error;
```
With this, user can do the following.
```ballerina
Person[] persons = check consumer->pollPayload(2);
```
With this new data binding improvement, the compiler plugin validation for `onConsumerRecord` function will also be updated to allow types of both `kafka:AnydataConsumerRecord[]` and `anydata[]`.
## Testing
- Testing the runtime data type conversions on the producer, consumer & listener.
- Testing compiler plugin validation to accept new data types.
