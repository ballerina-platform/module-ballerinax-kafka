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

// Common constants

# Data types for the serializer/deserializer functionality.
const STRING = "string";
const INT = "int";
const FLOAT = "float";
const BYTE_ARRAY = "byte[]";
const AVRO_RECORD = "kafka:AvroRecord";
const ANY = "anydata";

# The default server URL.
public const DEFAULT_URL = "localhost:9092";

// ********************************************
//         Consumer-Related constants         *
// ********************************************
// Deserializer types.
# In-built Kafka byte array deserializer.

# In-built Kafka string deserializer.
public const DES_STRING = "STRING";

# In-built Kafka int deserializer.
public const DES_INT = "INT";

# In-built Kafka float deserializer.
public const DES_FLOAT = "FLOAT";

# User-defined deserializer.
public const DES_CUSTOM = "CUSTOM";

// Isolation levels.
# Configures the consumer to read the committed messages only in the transactional mode when poll() is called.
public const ISOLATION_COMMITTED = "read_committed";

# Configures the consumer to read all the messages including the aborted ones.
public const ISOLATION_UNCOMMITTED = "read_uncommitted";

# Automatically reset the consumer offset to the earliest offset
public const OFFSET_RESET_EARLIEST = "earliest";

# Automatically reset the consumer offset to the latest offset
public const OFFSET_RESET_LATEST = "latest";

# If the `offsetReset` is set to `OFFSET_RESET_NONE`, the consumer will give an error if no previous offset is found
# for the consumer group
public const OFFSET_RESET_NONE = "none";

// ********************************************
//         Producer-Related constants         *
// ********************************************
// Produce Ack types.
# Producer acknowledgement type is 'all'. This will guarantee that the record will not be lost as long as at least one
# in-sync replica is alive.
public const ACKS_ALL = "all";

# Producer acknowledgement type '0'. If the acknowledgement type set to this, the producer will not wait for any
# acknowledgement from the server.
public const ACKS_NONE = "0";

# Producer acknowledgement type '1'. If the acknowledgement type set to this, the leader will write the record to its
# A local log will respond without waiting FOR full acknowledgement from all the followers.
public const ACKS_SINGLE = "1";

// Serializer types.

# In-built Kafka string serializer.
public const SER_STRING = "STRING";

# In-built Kafka int serializer.
public const SER_INT = "INT";

# In-built Kafka float serializer.
public const SER_FLOAT = "FLOAT";

# User-defined serializer.
public const SER_CUSTOM = "CUSTOM";

// Compression types.
# No compression.
public const COMPRESSION_NONE = "none";

# Kafka GZIP compression type.
public const COMPRESSION_GZIP = "gzip";

# Kafka Snappy compression type.
public const COMPRESSION_SNAPPY = "snappy";

# Kafka LZ4 compression type.
public const COMPRESSION_LZ4 = "lz4";

# Kafka ZSTD compression type.
public const COMPRESSION_ZSTD = "zstd";

// ********************************************
//              Common constants              *
// ********************************************
// SASL Authentication mechanisms
# Kafka SASL_PLAIN authentication mechanism
public const AUTH_SASL_PLAIN = "PLAIN";
# Kafka SASL_SCRAM authentication mechanism
public const AUTH_SASL_SCRAM_SHA_256 = "SCRAM-SHA-256";
public const AUTH_SASL_SCRAM_SHA_512 = "SCRAM-SHA-512";


// Security Protocols
# Represents Kafka un-authenticated, non-encrypted channel
public const PROTOCOL_PLAINTEXT = "PLAINTEXT";

# Represents Kafka authenticated, non-encrypted channel
public const PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT";

# Represents Kafka SASL authenticated, SSL channel
public const PROTOCOL_SASL_SSL = "SASL_SSL";

# Represents Kafka SSL channel
public const PROTOCOL_SSL = "SSL";
