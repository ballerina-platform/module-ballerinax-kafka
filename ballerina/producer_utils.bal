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

isolated function getHeaderValueAsByteArrayList(map<byte[]|byte[][]|string|string[]>? headers) returns [string, byte[]][] {
    [string, byte[]][] bytesHeaderList = [];
    if headers is map<byte[]|byte[][]|string|string[]> {
        foreach string key in headers.keys() {
            byte[]|byte[][]|string|string[] values = headers.get(key);
            if values is byte[] {
                bytesHeaderList.push([key, values]);
            } else if values is byte[][] {
                foreach byte[] headerValue in values {
                    bytesHeaderList.push([key, headerValue]);
                }
            } else if values is string {
                bytesHeaderList.push([key, values.toBytes()]);
            } else if values is string[] {
                foreach string headerValue in values {
                    bytesHeaderList.push([key, headerValue.toBytes()]);
                }
            }
        }
    }
    return bytesHeaderList;
}

isolated function getByteValue(anydata value) returns byte[] {
    if value is byte[] {
        return value;
    } else if value is xml {
        return value.toString().toBytes();
    } else if value is string {
        return value.toBytes();
    } else {
        return value.toJsonString().toBytes();
    }
}

isolated function sendExternal(Producer producer, byte[] value, string topic, [string, byte[]][] headers, byte[]? key,
        int? partition, int? timestamp) returns Error? = @java:Method {
    'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.Send"
} external;

isolated function sendWithMetadataExternal(Producer producer, byte[] value, string topic, [string, byte[]][] headers,
        byte[]? key, int? partition, int? timestamp) returns RecordMetadata|Error = @java:Method {
    'class: "io.ballerina.stdlib.kafka.nativeimpl.producer.Send"
} external;
