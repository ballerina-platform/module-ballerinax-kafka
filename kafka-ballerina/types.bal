// Consumer-related types
# Represents the differnet types of offset-reset mothods for Kafka consumer
public type OffsetResetMethod OFFSET_RESET_EARLIEST|OFFSET_RESET_LATEST|OFFSET_RESET_NONE;

# Kafka in-built deserializer type.
public type DeserializerType DES_BYTE_ARRAY;

# Kafka consumer isolation level type.
public type IsolationLevel ISOLATION_COMMITTED|ISOLATION_UNCOMMITTED;

// Producer-related types
# Kafka producer acknowledgement types.
public type ProducerAcks ACKS_ALL|ACKS_NONE|ACKS_SINGLE;

# Kafka in-built serializer types.
public type SerializerType SER_BYTE_ARRAY;

# Kafka compression types to compress the messages.
public type CompressionType COMPRESSION_NONE|COMPRESSION_GZIP|COMPRESSION_SNAPPY|COMPRESSION_LZ4|COMPRESSION_ZSTD;

// Authentication-related types
# Represents the supported Kafka SASL authentication mechanisms.
public type AuthenticationMechanism AUTH_SASL_PLAIN;

# Represents the supported security protocols for Kafka clients.
public type SecurityProtocol PROTOCOL_PLAINTEXT|PROTOCOL_SASL_PLAINTEXT|PROTOCOL_SASL_SSL|PROTOCOL_SSL;

# The Kafka service type
public type KafkaService service object {
    remote function onMessage(Caller caller, ConsumerRecord[] records);
    // To be completed when support for optional params in remote functions is available in lang
};