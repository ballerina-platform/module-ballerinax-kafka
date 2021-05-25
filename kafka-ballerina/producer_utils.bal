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

import ballerina/jballerina.java;

isolated function sendByteArrayValues(Producer producer, byte[] value, string topic, anydata? key, int? partition,
    int? timestamp, string keySerializerType) returns Error? {
    if (key is ()) {
        return sendByteArrayValuesNilKeys(producer, value, topic, partition, timestamp);
    }
    if (keySerializerType == SER_BYTE_ARRAY) {
        if (key is byte[]) {
            return sendByteArrayValuesByteArrayKeys(producer, value, topic, key, partition, timestamp);
        }
        panic getKeyTypeMismatchError(BYTE_ARRAY);
    }
}

isolated function producerCommitConsumer(Producer producer, Consumer consumer) returns Error? =
@java:Method {
    name: "commitConsumer",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

isolated function producerCommitConsumerOffsets(Producer producer, PartitionOffset[] offsets, string groupID)
returns Error? =
@java:Method {
    name: "commitConsumerOffsets",
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.ProducerActions"
} external;

//////////////////////////////////////////////////////////////////////////////////////
//              Different send functions to send different types of data            //
//                  Naming convention: send<ValueType><KeyType>                     //
//   Reason for this naming convention is that the key can be nil but value cannot  //
//////////////////////////////////////////////////////////////////////////////////////

 //Send byte[] values with different types of keys
isolated function sendByteArrayValuesNilKeys(Producer producer, byte[] value, string topic, int? partition = (),
    int? timestamp = ()) returns Error? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues"
} external;

isolated function sendByteArrayValuesByteArrayKeys(Producer producer, byte[] value, string topic, byte[] key,
    int? partition = (), int? timestamp = ()) returns Error? =
@java:Method {
    'class: "org.ballerinalang.messaging.kafka.nativeimpl.producer.SendByteArrayValues"
} external;
