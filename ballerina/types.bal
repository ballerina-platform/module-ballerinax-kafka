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

// Consumer-related types
# Represents the different types of offset-reset methods of the Kafka consumer.
public type OffsetResetMethod OFFSET_RESET_EARLIEST|OFFSET_RESET_LATEST|OFFSET_RESET_NONE;

# `kafka:Consumer` isolation level type.
public type IsolationLevel ISOLATION_COMMITTED|ISOLATION_UNCOMMITTED;

// Producer-related types
# `kafka:Producer` acknowledgement types.
public type ProducerAcks ACKS_ALL|ACKS_NONE|ACKS_SINGLE;

# Kafka compression types to compress the messages.
public type CompressionType COMPRESSION_NONE|COMPRESSION_GZIP|COMPRESSION_SNAPPY|COMPRESSION_LZ4|COMPRESSION_ZSTD;

// Authentication-related types
# Represents the supported Kafka SASL authentication mechanisms.
public type AuthenticationMechanism AUTH_SASL_PLAIN|AUTH_SASL_SCRAM_SHA_256|AUTH_SASL_SCRAM_SHA_512;

# Represents the supported security protocols for Kafka clients.
public type SecurityProtocol PROTOCOL_PLAINTEXT|PROTOCOL_SASL_PLAINTEXT|PROTOCOL_SASL_SSL|PROTOCOL_SSL;

# The Kafka service type.
public type Service distinct service object {
    // To be completed when support for optional params in remote functions is available in lang
};

public enum SerializerType {
    SER_BYTE_ARRAY,
    SER_AVRO
}

public enum DeserializerType {
    DES_BYTE_ARRAY,
    DES_AVRO
}


public type Deserializer object {
    public function configure(string schemaRegistryUrl) returns error?;
    public function deserialize(byte[] value) returns anydata|error;
};

class KafkaAvroDeserializer {
    *Deserializer;
    cregistry:Client registry;

    public function init(string baseUrl, map<anydata> originals, map<string> headers) returns error? {
        self.registry = check new({
            baseUrl,
            originals,
            headers
        });
    }

    public function configure(string schemaRegistryUrl) returns error? {
    }

    public function deserialize(byte[] value) returns anydata|error {
        return cavroserdes:deserialize(self.registry, value, anydata);
    }
};
