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

# Kafka in-built serializer types.
public type SerializerType SER_BYTE_ARRAY;

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

# Kafka in-built serializer types.
public enum SerializerType {
    SER_BYTE_ARRAY,
    SER_AVRO
}

# Kafka in-built deserializer type.
public enum DeserializerType {
    DES_BYTE_ARRAY,
    DES_AVRO
}

# Interface for serializing a given value
public type Serializer isolated object {

    # Serializes a given value using the provided schema
    # 
    # + value - Data to be serialized  
    # + schema - The schema used for serialization  
    # + subject - The subject under which the schema is registered (default: "subject")  
    # + return - The serialized `byte[]` on success, otherwise an error 
    public isolated function serialize(anydata value, string schema, string subject = "subject") returns byte[]|error;
};

# Implementation of the `Serializer` interface for Avro serialization
public isolated class AvroSerializer {
    *Serializer;
    private final cregistry:Client registry;
    private final string schema;

    public isolated function init(anydata & readonly schemaRegistryConfig, string schema) returns error? {
        self.registry = check initiateSchemaRegistry(schemaRegistryConfig);
        self.schema = schema;
    }
    
    public isolated function serialize(anydata value, string schema, string subject) returns byte[]|error {
        return cavroserdes:serialize(self.registry, schema, value, subject);
    }
}

# Interface for deserializing a given value
public type Deserializer object {

    # Deserializes the provided value
    # 
    # + value - Data to be deserialized  
    # + return - Deserialized value as `anydata` on success, otherwise an error  
    public function deserialize(byte[] value) returns anydata|error;
};


# Implementation of the `Deserializer` interface for Avro deserialization
public isolated class AvroDeserializer {
    *Deserializer;
    private final cregistry:Client registry;

    public isolated function init(anydata & readonly schemaRegistryConfig) returns error? {
        self.registry = check initiateSchemaRegistry(schemaRegistryConfig);
    }

    public isolated function deserialize(byte[] value) returns anydata|error {
        return cavroserdes:deserialize(self.registry, value, anydata);
    }
}

isolated function getSchemaRegistryConfig(anydata & readonly schemaRegistryConfig)
    returns cregistry:ConnectionConfig|error {
    do {
        return check schemaRegistryConfig.cloneWithType(cregistry:ConnectionConfig);
    } on fail error registryError {
        string errorMessage = string `The provided values for 'schemaRegistryConfig' 
            do not match the expected schema registry properties: ${registryError.message()}`;
        return error(errorMessage);
    }
}

isolated function initiateSchemaRegistry(anydata & readonly schemaRegistryConfig) returns cregistry:Client|error {
    do {
        return check new (check getSchemaRegistryConfig(schemaRegistryConfig));
    } on fail error err {
        return error(string `Error occurred while initializing the confluent registry: ${err.message()}`);
    }
}
