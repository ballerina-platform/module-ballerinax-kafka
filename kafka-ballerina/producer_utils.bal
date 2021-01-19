// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/java;

isolated function sendStringValues(Producer producer, string value, string topic, anydata? key, int? partition, int? timestamp,
    string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendStringValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendStringValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendStringValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendStringValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendStringValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendStringValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendStringValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function sendIntValues(Producer producer, int value, string topic, anydata? key, int? partition, int? timestamp,
    string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendIntValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendIntValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendIntValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendIntValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendIntValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendIntValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendIntValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function sendFloatValues(Producer producer, float value, string topic, anydata? key, int? partition, int? timestamp,
    string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendFloatValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendFloatValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendFloatValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendFloatValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendFloatValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendFloatValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendFloatValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function sendByteArrayValues(Producer producer, byte[] value, string topic, anydata? key, int? partition,
    int? timestamp, string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendByteArrayValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendByteArrayValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendByteArrayValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendByteArrayValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendByteArrayValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendByteArrayValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendByteArrayValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function sendAvroValues(Producer producer, AvroRecord value, string topic, anydata? key, int? partition, int? timestamp,
    string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendAvroValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendAvroValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendAvroValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendAvroValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendAvroValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendAvroValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendAvroValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function sendCustomValues(Producer producer, anydata value, string topic, anydata? key, int? partition, int? timestamp,
    string keySerializerType) returns ProducerError? {
    if (key is ()) {
        return sendCustomValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_STRING) {
        if (key is string) {
            return sendCustomValuesStringKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(STRING);
    }
    if (keySerializerType == SER_INT) {
        if (key is int) {
            return sendCustomValuesIntKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(INT);
    }
    if (keySerializerType == SER_FLOAT) {
        if (key is float) {
            return sendCustomValuesFloatKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(FLOAT);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendCustomValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
    if (keySerializerType == SER_AVRO) {
        if (key is AvroRecord) {
            return sendCustomValuesAvroKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(AVRO_RECORD);
    }
    if (keySerializerType == SER_CUSTOM) {
        return sendCustomValuesCustomKeys(producer, value, topic, key, partition, timestamp);
    }
}

isolated function producerInit(Producer producer) returns ProducerError? =
@java:Method {
    name: "init",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerClose(Producer producer) returns ProducerError? =
@java:Method {
    name: "close",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerCommitConsumer(Producer producer, Consumer consumer) returns ProducerError? =
@java:Method {
    name: "commitConsumer",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerCommitConsumerOffsets(Producer producer, PartitionOffset[] offsets, string groupID)
returns ProducerError? =
@java:Method {
    name: "commitConsumerOffsets",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerFlushRecords(Producer producer) returns ProducerError? =
@java:Method {
    name: "flushRecords",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerGetTopicPartitions(Producer producer, string topic) returns TopicPartition[]|ProducerError =
@java:Method {
    name: "getTopicPartitions",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

//////////////////////////////////////////////////////////////////////////////////////
//              Different send functions to send different types of data            //
//                  Naming convention: send<ValueType><KeyType>                     //
//   Reason for this naming convention is that the key can be nil but value cannot  //
//////////////////////////////////////////////////////////////////////////////////////

// Send string values with different types of keys
isolated function sendStringValuesNilKeys(Producer producer, string value, string topic, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesStringKeys(Producer producer, string value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesIntKeys(Producer producer, string value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "long",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesFloatKeys(Producer producer, string value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                "org.ballerinalang.jvm.api.values.BString", "double", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesByteArrayKeys(Producer producer, string value, string topic, byte[] key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.ArrayValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesAvroKeys(Producer producer, string value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.MapValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendStringValuesCustomKeys(Producer producer, string value, string topic, anydata key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendStringValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;

// Send int values with different types of keys
isolated function sendIntValuesNilKeys(Producer producer, int value, string topic, int? partition = (), int? timestamp = ())
returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesStringKeys(Producer producer, int value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesIntKeys(Producer producer, int value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString", "long",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesFloatKeys(Producer producer, int value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString", "double",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesByteArrayKeys(Producer producer, int value, string topic, byte[] key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.values.ArrayValue", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesAvroKeys(Producer producer, int value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.values.MapValue", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendIntValuesCustomKeys(Producer producer, int value, string topic, anydata key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendIntValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "long", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;

// Send float values with different types of keys
isolated function sendFloatValuesNilKeys(Producer producer, float value, string topic, int? partition = (), int? timestamp = ())
returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesStringKeys(Producer producer, float value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesIntKeys(Producer producer, float value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "long", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesFloatKeys(Producer producer, float value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "double", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesByteArrayKeys(Producer producer, float value, string topic, byte[] key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.values.ArrayValue", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesAvroKeys(Producer producer, float value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "org.ballerinalang.jvm.values.MapValue", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendFloatValuesCustomKeys(Producer producer, float value, string topic, anydata key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendFloatValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "double", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;

// Send byte[] values with different types of keys
isolated function sendByteArrayValuesNilKeys(Producer producer, byte[] value, string topic, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesStringKeys(Producer producer, byte[] value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesIntKeys(Producer producer, byte[] value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "long", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesFloatKeys(Producer producer, byte[] value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "double", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesByteArrayKeys(Producer producer, byte[] value, string topic, byte[] key,
    int? partition = (), int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.ArrayValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesAvroKeys(Producer producer, byte[] value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.MapValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendByteArrayValuesCustomKeys(Producer producer, byte[] value, string topic, anydata key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.ArrayValue",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;

// Sends Avro values with different types of keys
isolated function sendAvroValuesNilKeys(Producer producer, AvroRecord value, string topic, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesStringKeys(Producer producer, AvroRecord value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesIntKeys(Producer producer, AvroRecord value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "long", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesFloatKeys(Producer producer, AvroRecord value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "double", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesByteArrayKeys(Producer producer, AvroRecord value, string topic, byte[] key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.ArrayValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesAvroKeys(Producer producer, AvroRecord value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.MapValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendAvroValuesCustomKeys(Producer producer, AvroRecord value, string topic, any key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "org.ballerinalang.jvm.values.MapValue",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;

// Send custom type values with different types of keys
isolated function sendCustomValuesNilKeys(Producer producer, anydata value, string topic, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendCustomValuesStringKeys(Producer producer, anydata value, string topic, string key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.api.values.BString",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendCustomValuesIntKeys(Producer producer, anydata value, string topic, int key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "long", "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendCustomValuesFloatKeys(Producer producer, anydata value, string topic, float key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object", "org.ballerinalang.jvm.api.values.BString", "double",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendCustomValuesByteArrayKeys(Producer producer, anydata value, string topic, byte[] key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.ArrayValue",
                 "java.lang.Object", "java.lang.Object"]
} external;

isolated function sendCustomValuesAvroKeys(Producer producer, anydata value, string topic, AvroRecord key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendAvroKeys",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "org.ballerinalang.jvm.values.MapValue", "java.lang.Object",
                 "java.lang.Object"]
} external;

isolated function sendCustomValuesCustomKeys(Producer producer, anydata value, string topic, any key, int? partition = (),
    int? timestamp = ()) returns ProducerError? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendCustomValues",
    paramTypes: ["org.ballerinalang.jvm.values.ObjectValue", "java.lang.Object",
                 "org.ballerinalang.jvm.api.values.BString", "java.lang.Object", "java.lang.Object", "java.lang.Object"]
} external;
