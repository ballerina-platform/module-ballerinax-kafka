/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.kafka.utils;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;

/**
 * Constants related to for Kafka API.
 */
public class KafkaConstants {

    private KafkaConstants() {
    }

    public static final int DURATION_UNDEFINED_VALUE = -1;

    // Kafka log messages
    public static final String KAFKA_SERVERS = "[ballerinax/kafka] kafka servers: ";
    public static final String SUBSCRIBED_TOPICS = "[ballerinax/kafka] subscribed topics: ";

    public static final String NATIVE_CONSUMER = "KafkaConsumer";
    public static final String NATIVE_PRODUCER = "KafkaProducer";
    public static final String NATIVE_CONSUMER_CONFIG = "KafkaConsumerConfig";
    public static final String NATIVE_PRODUCER_CONFIG = "KafkaProducerConfig";
    public static final BString CONNECTOR_ID = StringUtils.fromString("connectorId");

    public static final String TRANSACTION_CONTEXT = "TransactionInitiated";

    public static final String TOPIC_PARTITION_TYPE_NAME = "TopicPartition";
    public static final String OFFSET_AND_TIMESTAMP_TYPE_NAME = "OffsetAndTimestamp";
    public static final String RECORD_METADATA_TYPE_NAME = "RecordMetadata";
    public static final String OFFSET_STRUCT_NAME = "PartitionOffset";

    public static final String KAFKA_ERROR = "Error";
    public static final String PAYLOAD_BINDING_ERROR = "PayloadBindingError";
    public static final String PAYLOAD_VALIDATION_ERROR = "PayloadValidationError";

    public static final String CALLER_STRUCT_NAME = "Caller";
    public static final String TYPE_CHECKER_OBJECT_NAME = "TypeChecker";
    public static final String SERVER_CONNECTOR = "serverConnector";
    public static final BString AUTO_COMMIT = StringUtils.fromString("autoCommit");

    public static final BString CONSUMER_CONFIG_FIELD_NAME = StringUtils.fromString("consumerConfig");
    public static final BString PRODUCER_CONFIG_FIELD_NAME = StringUtils.fromString("producerConfig");

    public static final String KAFKA_RESOURCE_ON_RECORD = "onConsumerRecord";
    public static final String KAFKA_RESOURCE_ON_ERROR = "onError";
    public static final String KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD = "isAnydataConsumerRecord";
    public static final String KAFKA_RECORD_KEY = "key";
    public static final String KAFKA_RECORD_VALUE = "value";
    public static final BString KAFKA_RECORD_TIMESTAMP = StringUtils.fromString("timestamp");
    public static final BString KAFKA_RECORD_PARTITION_OFFSET = StringUtils.fromString("offset");
    public static final BString KAFKA_RECORD_HEADERS = StringUtils.fromString("headers");

    public static final BString OFFSET_FIELD = StringUtils.fromString("offset");
    public static final BString TIMESTAMP_FIELD = StringUtils.fromString("timestamp");
    public static final BString LEADER_EPOCH_FIELD = StringUtils.fromString("leaderEpoch");

    public static final BString SERIALIZED_KEY_SIZE_FIELD = StringUtils.fromString("serializedKeySize");
    public static final BString SERIALIZED_VALUE_SIZE_FIELD = StringUtils.fromString("serializedValueSize");

    public static final String PARAM_ANNOTATION_PREFIX = "$param$.";
    public static final BString PARAM_PAYLOAD_ANNOTATION_NAME = StringUtils.fromString(
            ModuleUtils.getModule().toString() + ":Payload");

    public static final BString ADDITIONAL_PROPERTIES_MAP_FIELD = StringUtils.fromString("additionalProperties");

    public static final BString ALIAS_CONCURRENT_CONSUMERS = StringUtils.fromString("concurrentConsumers");
    public static final BString ALIAS_TOPICS = StringUtils.fromString("topics");
    public static final BString ALIAS_POLLING_TIMEOUT = StringUtils.fromString("pollingTimeout");
    public static final BString ALIAS_POLLING_INTERVAL = StringUtils.fromString("pollingInterval");
    public static final BString ALIAS_DECOUPLE_PROCESSING = StringUtils.fromString("decoupleProcessing");
    public static final BString ALIAS_TOPIC = StringUtils.fromString("topic");
    public static final BString ALIAS_PARTITION = StringUtils.fromString("partition");
    public static final BString ALIAS_OFFSET = StringUtils.fromString("offset");
    public static final String ALIAS_DURATION = "duration";

    // Consumer Configuration.
    public static final BString CONSUMER_BOOTSTRAP_SERVERS_CONFIG = StringUtils.fromString("bootstrapServers");
    public static final BString CONSUMER_GROUP_ID_CONFIG = StringUtils.fromString("groupId");
    public static final BString CONSUMER_AUTO_OFFSET_RESET_CONFIG = StringUtils.fromString("offsetReset");
    public static final BString CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_CONFIG = StringUtils.fromString(
            "partitionAssignmentStrategy");
    public static final BString CONSUMER_METRICS_RECORDING_LEVEL_CONFIG = StringUtils.fromString(
            "metricsRecordingLevel");
    public static final BString CONSUMER_METRIC_REPORTER_CLASSES_CONFIG = StringUtils.fromString(
            "metricsReporterClasses");
    public static final BString CONSUMER_CLIENT_ID_CONFIG = StringUtils.fromString("clientId");
    public static final BString CONSUMER_INTERCEPTOR_CLASSES_CONFIG = StringUtils.fromString("interceptorClasses");
    public static final BString CONSUMER_ISOLATION_LEVEL_CONFIG = StringUtils.fromString("isolationLevel");
    public static final BString CONSUMER_SCHEMA_REGISTRY_URL = StringUtils.fromString("schemaRegistryUrl");

    public static final BString CONSUMER_SESSION_TIMEOUT_MS_CONFIG = StringUtils.fromString("sessionTimeout");
    public static final BString CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = StringUtils.fromString(
            "heartBeatInterval");
    public static final BString CONSUMER_METADATA_MAX_AGE_CONFIG = StringUtils.fromString("metadataMaxAge");
    public static final BString CONSUMER_AUTO_COMMIT_INTERVAL_MS_CONFIG = StringUtils.fromString(
            "autoCommitInterval");
    public static final BString CONSUMER_MAX_PARTITION_FETCH_BYTES_CONFIG = StringUtils.fromString(
            "maxPartitionFetchBytes");
    public static final BString CONSUMER_SEND_BUFFER_CONFIG = StringUtils.fromString("sendBuffer");
    public static final BString CONSUMER_RECEIVE_BUFFER_CONFIG = StringUtils.fromString("receiveBuffer");
    public static final BString CONSUMER_FETCH_MIN_BYTES_CONFIG = StringUtils.fromString("fetchMinBytes");
    public static final BString CONSUMER_FETCH_MAX_BYTES_CONFIG = StringUtils.fromString("fetchMaxBytes");
    public static final BString CONSUMER_FETCH_MAX_WAIT_MS_CONFIG = StringUtils.fromString("fetchMaxWaitTime");
    public static final BString CONSUMER_RECONNECT_BACKOFF_MS_CONFIG = StringUtils.fromString(
            "reconnectBackoffTime");
    public static final BString CONSUMER_RETRY_BACKOFF_MS_CONFIG = StringUtils.fromString("retryBackoff");
    public static final BString CONSUMER_METRICS_SAMPLE_WINDOW_MS_CONFIG = StringUtils.fromString(
            "metricsSampleWindow");
    public static final BString CONSUMER_METRICS_NUM_SAMPLES_CONFIG = StringUtils.fromString("metricsNumSamples");
    public static final BString CONSUMER_REQUEST_TIMEOUT_MS_CONFIG = StringUtils.fromString("requestTimeout");
    public static final BString CONSUMER_CONNECTIONS_MAX_IDLE_MS_CONFIG = StringUtils.fromString(
            "connectionMaxIdleTime");
    public static final BString CONSUMER_MAX_POLL_RECORDS_CONFIG = StringUtils.fromString("maxPollRecords");
    public static final BString CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG = StringUtils.fromString("maxPollInterval");
    public static final BString CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG = StringUtils.fromString(
            "reconnectBackoffTimeMax");
    public static final BString CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = StringUtils.fromString("autoCommit");
    public static final BString CONSUMER_ENABLE_AUTO_SEEK_CONFIG = StringUtils
            .fromString("autoSeekOnValidationFailure");
    public static final BString CONSUMER_CHECK_CRCS_CONFIG = StringUtils.fromString("checkCRCS");
    public static final BString CONSUMER_EXCLUDE_INTERNAL_TOPICS_CONFIG = StringUtils.fromString(
            "excludeInternalTopics");
    public static final BString CONSUMER_DEFAULT_API_TIMEOUT_CONFIG = StringUtils.fromString(
            "defaultApiTimeout");
    public static final BString CONSUMER_ENABLE_AUTO_COMMIT = StringUtils.fromString("enable.auto.commit");

    // Producer Configuration.
    public static final BString PRODUCER_BOOTSTRAP_SERVERS_CONFIG = StringUtils.fromString("bootstrapServers");
    public static final BString PRODUCER_ACKS_CONFIG = StringUtils.fromString("acks");
    public static final BString PRODUCER_COMPRESSION_TYPE_CONFIG = StringUtils.fromString("compressionType");
    public static final BString PRODUCER_CLIENT_ID_CONFIG = StringUtils.fromString("clientId");
    public static final BString PRODUCER_METRICS_RECORDING_LEVEL_CONFIG = StringUtils.fromString(
            "metricsRecordingLevel");
    public static final BString PRODUCER_METRIC_REPORTER_CLASSES_CONFIG = StringUtils.fromString(
            "metricReporterClasses");
    public static final BString PRODUCER_PARTITIONER_CLASS_CONFIG = StringUtils.fromString("partitionerClass");
    public static final BString PRODUCER_INTERCEPTOR_CLASSES_CONFIG = StringUtils.fromString("interceptorClasses");
    public static final BString PRODUCER_TRANSACTIONAL_ID_CONFIG = StringUtils.fromString("transactionalId");
    public static final BString PRODUCER_SCHEMA_REGISTRY_URL = StringUtils.fromString("schemaRegistryUrl");
    public static final BString PRODUCER_BUFFER_MEMORY_CONFIG = StringUtils.fromString("bufferMemory");
    public static final BString PRODUCER_RETRIES_CONFIG = StringUtils.fromString("retryCount");
    public static final BString PRODUCER_BATCH_SIZE_CONFIG = StringUtils.fromString("batchSize");
    public static final BString PRODUCER_LINGER_MS_CONFIG = StringUtils.fromString("linger");
    public static final BString PRODUCER_SEND_BUFFER_CONFIG = StringUtils.fromString("sendBuffer");
    public static final BString PRODUCER_RECEIVE_BUFFER_CONFIG = StringUtils.fromString("receiveBuffer");
    public static final BString PRODUCER_MAX_REQUEST_SIZE_CONFIG = StringUtils.fromString("maxRequestSize");
    public static final BString PRODUCER_RECONNECT_BACKOFF_MS_CONFIG = StringUtils.fromString(
            "reconnectBackoffTime");
    public static final BString PRODUCER_RECONNECT_BACKOFF_MAX_MS_CONFIG = StringUtils.fromString(
            "reconnectBackoffMaxTime");
    public static final BString PRODUCER_RETRY_BACKOFF_MS_CONFIG = StringUtils.fromString("retryBackoffTime");
    public static final BString PRODUCER_MAX_BLOCK_MS_CONFIG = StringUtils.fromString("maxBlock");
    public static final BString PRODUCER_REQUEST_TIMEOUT_MS_CONFIG = StringUtils.fromString("requestTimeout");
    public static final BString PRODUCER_METADATA_MAX_AGE_CONFIG = StringUtils.fromString("metadataMaxAge");
    public static final BString PRODUCER_METRICS_SAMPLE_WINDOW_MS_CONFIG = StringUtils.fromString(
            "metricsSampleWindow");
    public static final BString PRODUCER_METRICS_NUM_SAMPLES_CONFIG = StringUtils.fromString("metricsNumSamples");
    public static final BString PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = StringUtils.fromString(
            "maxInFlightRequestsPerConnection");
    public static final BString PRODUCER_CONNECTIONS_MAX_IDLE_MS_CONFIG = StringUtils.fromString(
            "connectionsMaxIdleTime");
    public static final BString PRODUCER_TRANSACTION_TIMEOUT_CONFIG = StringUtils.fromString(
            "transactionTimeout");
    public static final BString PRODUCER_ENABLE_IDEMPOTENCE_CONFIG = StringUtils.fromString("enableIdempotence");

    // SSL Configuration parameters.
    public static final BString SECURE_SOCKET = StringUtils.fromString("secureSocket");
    public static final BString KEYSTORE_CONFIG = StringUtils.fromString("keyStore");
    public static final BString KEY_CONFIG = StringUtils.fromString("key");
    public static final BString TRUSTSTORE_CONFIG = StringUtils.fromString("cert");
    public static final BString PROTOCOL_CONFIG = StringUtils.fromString("protocol");
    public static final BString LOCATION_CONFIG = StringUtils.fromString("path");
    public static final BString PASSWORD_CONFIG = StringUtils.fromString("password");
    public static final BString SSL_PROTOCOL_VERSIONS = StringUtils.fromString("versions");
    public static final BString SECURITY_PROTOCOL_CONFIG = StringUtils.fromString("securityProtocol");
    public static final BString SSL_PROTOCOL_NAME = StringUtils.fromString("name");
    public static final BString SSL_PROVIDER_CONFIG = StringUtils.fromString("provider");
    public static final BString SSL_KEY_PASSWORD_CONFIG = StringUtils.fromString("keyPassword");
    public static final BString SSL_CIPHER_SUITES_CONFIG = StringUtils.fromString("ciphers");
    public static final BString SSL_CERT_FILE_LOCATION_CONFIG = StringUtils.fromString("certFile");
    public static final BString SSL_KEY_FILE_LOCATION_CONFIG = StringUtils.fromString("keyFile");

    // SASL Configuration parameters
    public static final BString AUTHENTICATION_CONFIGURATION = StringUtils.fromString("auth");
    public static final BString AUTHENTICATION_MECHANISM = StringUtils.fromString("mechanism");
    public static final BString USERNAME = StringUtils.fromString("username");
    public static final BString PASSWORD = StringUtils.fromString("password");

    // Authentication Mechanisms
    public static final String SASL_PLAIN = "PLAIN";
    public static final String SASL_SCRAM_SHA_256 = "SCRAM-SHA-256";
    public static final String SASL_SCRAM_SHA_512 = "SCRAM-SHA-512";

    // Avro Deserialization names
    public static final String AVRO_DESERIALIZATION_TYPE = "DES_AVRO";
    public static final String DESERIALIZE_FUNCTION = "deserialize";
    public static final String KEY_DESERIALIZER = "keyDeserializer";
    public static final String KEY_DESERIALIZER_TYPE = "keyDeserializerType";
    public static final String VALUE_DESERIALIZER = "valueDeserializer";
    public static final String VALUE_DESERIALIZER_TYPE = "valueDeserializerType";


    // Default class names
    // Serializers
    public static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

    // Deserializers
    public static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    // Warning suppression
    public static final String UNCHECKED = "unchecked";

    //Properties constants
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String CLIENT_ID = "client.id";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String DEFAULT_SER_DES_TYPE = "BYTE_ARRAY";
    public static final BString CONSTRAINT_VALIDATION = StringUtils.fromString("validation");

    public static final BigDecimal MILLISECOND_MULTIPLIER = new BigDecimal(1000);

    // SSL keystore/truststore type config
    public static final String SSL_STORE_TYPE_CONFIG = "PEM";

    public static final String APACHE_KAFKA_PACKAGE_NAME = "org.apache.kafka";
    public static final String BALLERINA_KAFKA_PACKAGE_NAME = "io.ballerina.stdlib.kafka";
}
