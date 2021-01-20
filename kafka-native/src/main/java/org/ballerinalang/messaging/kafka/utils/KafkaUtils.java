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

package org.ballerinalang.messaging.kafka.utils;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.async.Callback;
import io.ballerina.runtime.api.async.StrandMetadata;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.ballerinalang.messaging.kafka.observability.KafkaMetricsUtil;
import org.ballerinalang.messaging.kafka.observability.KafkaObservabilityConstants;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.ballerinalang.messaging.kafka.utils.AvroUtils.handleAvroConsumer;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.CONSUMER_ERROR;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;

/**
 * Utility class for Kafka Connector Implementation.
 */
public class KafkaUtils {

    private KafkaUtils() {
    }

    public static Object[] getResourceParameters(BObject service, BObject listener,
                                                 ConsumerRecords records) {

        BArray consumerRecordsArray = ValueCreator.createArrayValue(
                TypeCreator.createArrayType(getConsumerRecord().getType()));
        String keyType = KafkaConstants.DEFAULT_SER_DES_TYPE;
        String valueType = KafkaConstants.DEFAULT_SER_DES_TYPE;

        if (service.getType().getMethods()[0].getParameterTypes().length == 2) {
            for (Object record : records) {
                BMap<BString, Object> consumerRecord = populateConsumerRecord(
                        (ConsumerRecord) record, keyType, valueType);
                consumerRecordsArray.append(consumerRecord);
            }
            BObject caller =
                    ValueCreator.createObjectValue(ModuleUtils.getModule(), KafkaConstants.CALLER_STRUCT_NAME);
            KafkaConsumer consumer = (KafkaConsumer) listener.getNativeData(NATIVE_CONSUMER);
            Properties consumerProperties = (Properties) listener.getNativeData(NATIVE_CONSUMER_CONFIG);
            caller.addNativeData(NATIVE_CONSUMER, consumer);
            caller.addNativeData(NATIVE_CONSUMER_CONFIG, consumerProperties);
            return new Object[]{caller, true, consumerRecordsArray, true};
        } else {
            throw KafkaUtils.createKafkaError("Invalid remote function signature", CONSUMER_ERROR);
        }
    }

    public static Properties processKafkaConsumerConfig(BMap<BString, Object> configurations) {
        Properties properties = new Properties();

        addStringParamIfPresent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
        addStringParamIfPresent(ConsumerConfig.GROUP_ID_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_GROUP_ID_CONFIG);
        addStringParamIfPresent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        addStringParamIfPresent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        addStringParamIfPresent(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_METRICS_RECORDING_LEVEL_CONFIG);
        addStringParamIfPresent(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_METRIC_REPORTER_CLASSES_CONFIG);
        addStringParamIfPresent(ConsumerConfig.CLIENT_ID_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_CLIENT_ID_CONFIG);
        addStringParamIfPresent(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_INTERCEPTOR_CLASSES_CONFIG);
        addStringParamIfPresent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, configurations, properties,
                                KafkaConstants.CONSUMER_ISOLATION_LEVEL_CONFIG);

        addDeserializerConfigs(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties);
        addDeserializerConfigs(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties);
        // TODO: Disabled as the custom SerDes support is to be revisited and improved.
        //  Fix once the design for that is completed.
//        addCustomDeserializer(KafkaConstants.CONSUMER_KEY_DESERIALIZER_CONFIG,
//                              KafkaConstants.CONSUMER_KEY_DESERIALIZER_TYPE_CONFIG, properties,
//                              configurations);
//        addCustomDeserializer(KafkaConstants.CONSUMER_VALUE_DESERIALIZER_CONFIG,
//                              KafkaConstants.CONSUMER_VALUE_DESERIALIZER_TYPE_CONFIG, properties,
//                              configurations);
        addStringParamIfPresent(KafkaConstants.SCHEMA_REGISTRY_URL, configurations, properties,
                                KafkaConstants.CONSUMER_SCHEMA_REGISTRY_URL);

        addStringArrayParamIfPresent(KafkaConstants.ALIAS_TOPICS.getValue(), configurations, properties,
                                     KafkaConstants.ALIAS_TOPICS);

        addIntParamIfPresent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.METADATA_MAX_AGE_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_METADATA_MAX_AGE_CONFIG);
        addIntParamIfPresent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_AUTO_COMMIT_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_MAX_PARTITION_FETCH_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.SEND_BUFFER_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_SEND_BUFFER_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECEIVE_BUFFER_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_RECEIVE_BUFFER_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_FETCH_MIN_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_FETCH_MAX_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_RETRY_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_METRICS_SAMPLE_WINDOW_MS_CONFIG);

        addIntParamIfPresent(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_METRICS_NUM_SAMPLES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_CONNECTIONS_MAX_IDLE_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_MAX_POLL_RECORDS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, configurations, properties,
                             KafkaConstants.CONSUMER_DEFAULT_API_TIMEOUT_CONFIG);

        addIntParamIfPresent(KafkaConstants.ALIAS_POLLING_TIMEOUT.getValue(), configurations, properties,
                             KafkaConstants.ALIAS_POLLING_TIMEOUT);
        addIntParamIfPresent(KafkaConstants.ALIAS_POLLING_INTERVAL.getValue(), configurations, properties,
                             KafkaConstants.ALIAS_POLLING_INTERVAL);
        addIntParamIfPresent(KafkaConstants.ALIAS_CONCURRENT_CONSUMERS.getValue(), configurations, properties,
                             KafkaConstants.ALIAS_CONCURRENT_CONSUMERS);

        addBooleanParamIfPresent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, configurations, properties,
                                 KafkaConstants.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, true);
        addBooleanParamIfPresent(ConsumerConfig.CHECK_CRCS_CONFIG, configurations, properties,
                                 KafkaConstants.CONSUMER_CHECK_CRCS_CONFIG, true);
        addBooleanParamIfPresent(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, configurations, properties,
                                 KafkaConstants.CONSUMER_EXCLUDE_INTERNAL_TOPICS_CONFIG, true);

        addBooleanParamIfPresent(KafkaConstants.ALIAS_DECOUPLE_PROCESSING.getValue(), configurations, properties,
                                 KafkaConstants.ALIAS_DECOUPLE_PROCESSING, false);
        if (Objects.nonNull(configurations.get(KafkaConstants.SECURE_SOCKET))) {
            processSslProperties(configurations, properties);
        }

        if (Objects.nonNull(configurations.get(KafkaConstants.AUTHENTICATION_CONFIGURATION))) {
            processSaslProperties(configurations, properties);
        }
        if (Objects.nonNull(configurations.getMapValue(KafkaConstants.ADDITIONAL_PROPERTIES_MAP_FIELD))) {
            processAdditionalProperties(configurations.getMapValue(KafkaConstants.ADDITIONAL_PROPERTIES_MAP_FIELD),
                                        properties);
        }
        return properties;
    }

    public static Properties processKafkaProducerConfig(BMap<BString, Object> configurations) {
        Properties properties = new Properties();
        addStringParamIfPresent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_BOOTSTRAP_SERVERS_CONFIG);
        addStringParamIfPresent(ProducerConfig.ACKS_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_ACKS_CONFIG);
        addStringParamIfPresent(ProducerConfig.COMPRESSION_TYPE_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_COMPRESSION_TYPE_CONFIG);
        addStringParamIfPresent(ProducerConfig.CLIENT_ID_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_CLIENT_ID_CONFIG);
        addStringParamIfPresent(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_METRICS_RECORDING_LEVEL_CONFIG);
        addStringParamIfPresent(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_METRIC_REPORTER_CLASSES_CONFIG);
        addStringParamIfPresent(ProducerConfig.PARTITIONER_CLASS_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_PARTITIONER_CLASS_CONFIG);
        addStringParamIfPresent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_INTERCEPTOR_CLASSES_CONFIG);
        addStringParamIfPresent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, configurations,
                                properties, KafkaConstants.PRODUCER_TRANSACTIONAL_ID_CONFIG);
        addStringParamIfPresent(KafkaConstants.SCHEMA_REGISTRY_URL, configurations, properties,
                                KafkaConstants.PRODUCER_SCHEMA_REGISTRY_URL);

        addSerializerTypeConfigs(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties);
        addSerializerTypeConfigs(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties);
        // TODO: Disabled as the custom SerDes support is to be revisited and improved.
        //  Fix once the design for that is completed.
//        addCustomKeySerializer(properties, configurations);
//        addCustomValueSerializer(properties, configurations);

        addIntParamIfPresent(ProducerConfig.BUFFER_MEMORY_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_BUFFER_MEMORY_CONFIG);
        addIntParamIfPresent(ProducerConfig.RETRIES_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_RETRIES_CONFIG);
        addIntParamIfPresent(ProducerConfig.BATCH_SIZE_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_BATCH_SIZE_CONFIG);
        addIntParamIfPresent(ProducerConfig.LINGER_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_LINGER_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.SEND_BUFFER_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_SEND_BUFFER_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECEIVE_BUFFER_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_RECEIVE_BUFFER_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_MAX_REQUEST_SIZE_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_RECONNECT_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_RETRY_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_BLOCK_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_MAX_BLOCK_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_REQUEST_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.METADATA_MAX_AGE_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_METADATA_MAX_AGE_CONFIG);
        addIntParamIfPresent(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_METRICS_SAMPLE_WINDOW_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_METRICS_NUM_SAMPLES_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, configurations,
                             properties, KafkaConstants.PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        addIntParamIfPresent(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_CONNECTIONS_MAX_IDLE_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, configurations,
                             properties, KafkaConstants.PRODUCER_TRANSACTION_TIMEOUT_CONFIG);

        addBooleanParamIfPresent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, configurations,
                                 properties, KafkaConstants.PRODUCER_ENABLE_IDEMPOTENCE_CONFIG);
        if (Objects.nonNull(configurations.get(KafkaConstants.SECURE_SOCKET))) {
            processSslProperties(configurations, properties);
        }
        if (Objects.nonNull(configurations.get(KafkaConstants.AUTHENTICATION_CONFIGURATION))) {
            processSaslProperties(configurations, properties);
        }
        if (Objects.nonNull(configurations.getMapValue(KafkaConstants.ADDITIONAL_PROPERTIES_MAP_FIELD))) {
            processAdditionalProperties(configurations.getMapValue(KafkaConstants.ADDITIONAL_PROPERTIES_MAP_FIELD),
                                        properties);
        }
        return properties;
    }

    @SuppressWarnings(KafkaConstants.UNCHECKED)
    private static void processSslProperties(BMap<BString, Object> configurations, Properties configParams) {
        BMap<BString, Object> secureSocket = (BMap<BString, Object>) configurations.get(
                KafkaConstants.SECURE_SOCKET);
        addStringParamIfPresent(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.KEYSTORE_CONFIG), configParams,
                                KafkaConstants.KEYSTORE_TYPE_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.KEYSTORE_CONFIG), configParams,
                                KafkaConstants.LOCATION_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.KEYSTORE_CONFIG), configParams,
                                KafkaConstants.PASSWORD_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.KEYSTORE_CONFIG), configParams,
                                KafkaConstants.KEYMANAGER_ALGORITHM_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.TRUSTSTORE_CONFIG),
                                configParams, KafkaConstants.TRUSTSTORE_TYPE_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.TRUSTSTORE_CONFIG),
                                configParams, KafkaConstants.LOCATION_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.TRUSTSTORE_CONFIG),
                                configParams, KafkaConstants.PASSWORD_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.TRUSTSTORE_CONFIG),
                                configParams, KafkaConstants.TRUSTMANAGER_ALGORITHM_CONFIG);
        addStringParamIfPresent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.PROTOCOL_CONFIG), configParams,
                                KafkaConstants.SECURITY_PROTOCOL_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_PROTOCOL_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.PROTOCOL_CONFIG), configParams,
                                KafkaConstants.SSL_PROTOCOL_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                                (BMap<BString, Object>) secureSocket.get(KafkaConstants.PROTOCOL_CONFIG), configParams,
                                KafkaConstants.ENABLED_PROTOCOLS_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_PROVIDER_CONFIG, configurations, configParams,
                                KafkaConstants.SSL_PROVIDER_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_KEY_PASSWORD_CONFIG, configurations, configParams,
                                KafkaConstants.SSL_KEY_PASSWORD_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_CIPHER_SUITES_CONFIG, configurations, configParams,
                                KafkaConstants.SSL_CIPHER_SUITES_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, configurations, configParams,
                                KafkaConstants.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        addStringParamIfPresent(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, configurations, configParams,
                                KafkaConstants.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
    }

    @SuppressWarnings(KafkaConstants.UNCHECKED)
    private static void processSaslProperties(BMap<BString, Object> configurations, Properties properties) {
        BMap<BString, Object> authenticationConfig =
                (BMap<BString, Object>) configurations.getMapValue(KafkaConstants.AUTHENTICATION_CONFIGURATION);
        String mechanism = authenticationConfig.getStringValue(KafkaConstants.AUTHENTICATION_MECHANISM).getValue();
        if (KafkaConstants.SASL_PLAIN.equals(mechanism)) {
            String username = authenticationConfig.getStringValue(KafkaConstants.USERNAME).getValue();
            String password = authenticationConfig.getStringValue(KafkaConstants.PASSWORD).getValue();
            String jaasConfigValue =
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username +
                            "\" password=\"" + password + "\";";
            addStringParamIfPresent(SaslConfigs.SASL_MECHANISM, authenticationConfig, properties,
                                    KafkaConstants.AUTHENTICATION_MECHANISM);
            addStringParamIfPresent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, authenticationConfig, properties,
                                    KafkaConstants.SECURITY_PROTOCOL_CONFIG);
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigValue);
        }
    }

    private static void processAdditionalProperties(BMap propertiesMap, Properties kafkaProperties) {
        for (Object key : propertiesMap.getKeys()) {
            kafkaProperties.setProperty(key.toString(), propertiesMap.getStringValue((BString) key).getValue());
        }
    }

    private static void addSerializerTypeConfigs(String paramName, Properties configParams) {
        configParams.put(paramName, KafkaConstants.BYTE_ARRAY_SERIALIZER);
    }

    private static void addDeserializerConfigs(String paramName, Properties configParams) {
            configParams.put(paramName, KafkaConstants.BYTE_ARRAY_DESERIALIZER);
    }

    // TODO: Disabled as the SerDes support is to be revisited and improved. Fix once the design for that is completed.
//    private static void addCustomKeySerializer(Properties properties, BMap<BString, Object> configurations) {
//        Object serializer = configurations.get(KafkaConstants.PRODUCER_KEY_SERIALIZER_CONFIG);
//        String serializerType =
//                configurations.getStringValue(KafkaConstants.PRODUCER_KEY_SERIALIZER_TYPE_CONFIG).getValue();
//        if (Objects.nonNull(serializer) && KafkaConstants.SERDES_CUSTOM.equals(serializerType)) {
//            properties.put(KafkaConstants.PRODUCER_KEY_SERIALIZER_CONFIG.getValue(),
//                           configurations.get(KafkaConstants.PRODUCER_KEY_SERIALIZER_CONFIG));
//        }
//    }
//
//    private static void addCustomValueSerializer(Properties properties, BMap<BString, Object> configurations) {
//        Object serializer = configurations.get(KafkaConstants.PRODUCER_VALUE_SERIALIZER_CONFIG);
//        String serializerType =
//                configurations.getStringValue(KafkaConstants.PRODUCER_VALUE_SERIALIZER_TYPE_CONFIG).getValue();
//        if (Objects.nonNull(serializer) && KafkaConstants.SERDES_CUSTOM.equals(serializerType)) {
//            properties.put(KafkaConstants.PRODUCER_VALUE_SERIALIZER_CONFIG.getValue(),
//                           configurations.get(KafkaConstants.PRODUCER_VALUE_SERIALIZER_CONFIG));
//        }
//    }

//    private static void addCustomDeserializer(BString configParam, BString typeConfig, Properties properties,
//                                              BMap<BString, Object> configurations) {
//        Object deserializer = configurations.get(configParam);
//        String deserializerType = configurations.getStringValue(typeConfig).getValue();
//        if (Objects.nonNull(deserializer) && KafkaConstants.SERDES_CUSTOM.equals(deserializerType)) {
//            properties.put(configParam.getValue(), configurations.get(configParam));
//            properties.put(KafkaConstants.BALLERINA_STRAND, Runtime.getCurrentRuntime());
//        }
//    }

//    private static String getSerializerType(String value) {
//        switch (value) {
//            case KafkaConstants.SERDES_BYTE_ARRAY:
//                return KafkaConstants.BYTE_ARRAY_SERIALIZER;
//            case KafkaConstants.SERDES_STRING:
//                return KafkaConstants.STRING_SERIALIZER;
//            case KafkaConstants.SERDES_INT:
//                return KafkaConstants.INT_SERIALIZER;
//            case KafkaConstants.SERDES_FLOAT:
//                return KafkaConstants.FLOAT_SERIALIZER;
//            case KafkaConstants.SERDES_AVRO:
//                return KafkaConstants.AVRO_SERIALIZER;
//            case KafkaConstants.SERDES_CUSTOM:
//                return KafkaConstants.CUSTOM_SERIALIZER;
//            default:
//                return value;
//        }
//    }
//
//    private static String getDeserializerValue(String value) {
//        switch (value) {
//            case KafkaConstants.SERDES_BYTE_ARRAY:
//                return KafkaConstants.BYTE_ARRAY_DESERIALIZER;
//            case KafkaConstants.SERDES_STRING:
//                return KafkaConstants.STRING_DESERIALIZER;
//            case KafkaConstants.SERDES_INT:
//                return KafkaConstants.INT_DESERIALIZER;
//            case KafkaConstants.SERDES_FLOAT:
//                return KafkaConstants.FLOAT_DESERIALIZER;
//            case KafkaConstants.SERDES_AVRO:
//                return KafkaConstants.AVRO_DESERIALIZER;
//            case KafkaConstants.SERDES_CUSTOM:
//                return KafkaConstants.CUSTOM_DESERIALIZER;
//            default:
//                return value;
//        }
//    }

    private static void addStringParamIfPresent(String paramName,
                                                BMap<BString, Object> configs,
                                                Properties configParams,
                                                BString key) {
        if (Objects.nonNull(configs.get(key))) {
            BString value = (BString) configs.get(key);
            if (!(value == null || value.getValue().equals(""))) {
                configParams.setProperty(paramName, value.getValue());
            }
        }
    }

    private static void addStringArrayParamIfPresent(String paramName,
                                                     BMap<BString, Object> configs,
                                                     Properties configParams,
                                                     BString key) {
        BArray stringArray = (BArray) configs.get(key);
        List<String> values = getStringListFromStringBArray(stringArray);
        configParams.put(paramName, values);
    }

    private static void addIntParamIfPresent(String paramName,
                                             BMap<BString, Object> configs,
                                             Properties configParams,
                                             BString key) {
        Long value = (Long) configs.get(key);
        if (Objects.nonNull(value)) {
            configParams.put(paramName, value.intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName,
                                                 BMap<BString, Object> configs,
                                                 Properties configParams,
                                                 BString key,
                                                 boolean defaultValue) {
        boolean value = (boolean) configs.get(key);
        if (value != defaultValue) {
            configParams.put(paramName, value);
        }
    }

    private static void addBooleanParamIfPresent(String paramName,
                                                 BMap<BString, Object> configs,
                                                 Properties configParams,
                                                 BString key) {
        boolean value = (boolean) configs.get(key);
        configParams.put(paramName, value);
    }

    public static ArrayList<TopicPartition> getTopicPartitionList(BArray partitions, Logger logger) {
        ArrayList<TopicPartition> partitionList = new ArrayList<>();
        if (partitions != null) {
            for (int counter = 0; counter < partitions.size(); counter++) {
                BMap<BString, Object> partition = (BMap<BString, Object>) partitions.get(counter);
                String topic = partition.get(KafkaConstants.ALIAS_TOPIC).toString();
                int partitionValue = getIntFromLong((Long) partition.get(KafkaConstants.ALIAS_PARTITION), logger,
                                                    KafkaConstants.ALIAS_PARTITION.getValue());
                partitionList.add(new TopicPartition(topic, partitionValue));
            }
        }
        return partitionList;
    }

    public static List<String> getStringListFromStringBArray(BArray stringArray) {
        ArrayList<String> values = new ArrayList<>();
        if ((Objects.isNull(stringArray)) ||
                (!((ArrayType) stringArray.getType()).getElementType().equals(PredefinedTypes.TYPE_STRING))) {
            return values;
        }
        if (stringArray.size() != 0) {
            for (int i = 0; i < stringArray.size(); i++) {
                values.add(stringArray.getString(i));
            }
        }
        return values;
    }

    /**
     * Populate the {@code TopicPartition} record type in Ballerina.
     *
     * @param topic     name of the topic
     * @param partition value of the partition offset
     * @return {@code BMap} of the record
     */
    public static BMap<BString, Object> populateTopicPartitionRecord(String topic, int partition) {
        return ValueCreator.createRecordValue(getTopicPartitionRecord(), topic, partition);
    }

    public static BMap<BString, Object> populatePartitionOffsetRecord(BMap<BString, Object> topicPartition,
                                                                          long offset) {
        return ValueCreator.createRecordValue(getPartitionOffsetRecord(), topicPartition, offset);
    }

    public static BMap<BString, Object> populateConsumerRecord(ConsumerRecord record, String keyType,
                                                                   String valueType) {
        Object key = null;
        if (Objects.nonNull(record.key())) {
            key = getBValues(record.key(), keyType);
        }

        Object value = getBValues(record.value(), valueType);
        Object[] fields = new Object[4];
        fields[0] = key;
        fields[1] = value;
        fields[2] = record.timestamp();
        BMap<BString, Object> topicPartition = ValueCreator.createRecordValue(getTopicPartitionRecord(), record.topic(),
                                                                              record.partition());
        fields[3] = ValueCreator.createRecordValue(getPartitionOffsetRecord(), topicPartition, record.offset());
        return ValueCreator.createRecordValue(getConsumerRecord(), fields);
    }

    private static Object getBValues(Object value, String type) {
        if (KafkaConstants.SERDES_BYTE_ARRAY.equals(type)) {
            if (value instanceof byte[]) {
                return ValueCreator.createArrayValue((byte[]) value);
            } else {
                throw createKafkaError(CONSUMER_ERROR, "Invalid type - expected: byte[]");
            }
        } else if (KafkaConstants.SERDES_STRING.equals(type)) {
            if (value instanceof String) {
                return StringUtils.fromString((String) value);
            } else {
                throw createKafkaError(CONSUMER_ERROR, "Invalid type - expected: string");
            }
        } else if (KafkaConstants.SERDES_INT.equals(type)) {
            if (value instanceof Long) {
                return value;
            } else {
                throw createKafkaError(CONSUMER_ERROR, "Invalid type - expected: int");
            }
        } else if (KafkaConstants.SERDES_FLOAT.equals(type)) {
            if (value instanceof Double) {
                return value;
            } else {
                throw createKafkaError(CONSUMER_ERROR, "Invalid type - expected: float");
            }
        } else if (KafkaConstants.SERDES_AVRO.equals(type)) {
            return handleAvroConsumer(value);
        } else if (KafkaConstants.SERDES_CUSTOM.equals(type)) {
            return value;
        }
        throw createKafkaError("Unexpected type found for consumer record", CONSUMER_ERROR);
    }

    public static BMap<BString, Object> getConsumerRecord() {
        return createKafkaRecord(KafkaConstants.CONSUMER_RECORD_STRUCT_NAME);
    }

    public static BMap<BString, Object> getAvroGenericRecord() {
        return createKafkaRecord(KafkaConstants.AVRO_GENERIC_RECORD_NAME);
    }

    public static BMap<BString, Object> getPartitionOffsetRecord() {
        return createKafkaRecord(KafkaConstants.OFFSET_STRUCT_NAME);
    }

    public static BMap<BString, Object> getTopicPartitionRecord() {
        return createKafkaRecord(KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
    }

    public static BError createKafkaError(String message, String typeId) {
        return ErrorCreator.createDistinctError(typeId, ModuleUtils.getModule(),
                                                StringUtils.fromString(message));
    }

    public static BError createKafkaError(String message, String typeId, BError cause) {
        return ErrorCreator.createDistinctError(typeId, ModuleUtils.getModule(),
                                                 StringUtils.fromString(message), cause);
    }

    public static BMap<BString, Object> createKafkaRecord(String recordName) {
        return ValueCreator.createRecordValue(ModuleUtils.getModule(), recordName);
    }

    public static BArray getPartitionOffsetArrayFromOffsetMap(Map<TopicPartition, Long> offsetMap) {
        BArray partitionOffsetArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(
                getPartitionOffsetRecord().getType()));
        if (!offsetMap.entrySet().isEmpty()) {
            for (Map.Entry<TopicPartition, Long> entry : offsetMap.entrySet()) {
                TopicPartition tp = entry.getKey();
                Long offset = entry.getValue();
                BMap<BString, Object> topicPartition = populateTopicPartitionRecord(tp.topic(), tp.partition());
                BMap<BString, Object> partition = populatePartitionOffsetRecord(topicPartition, offset);
                partitionOffsetArray.append(partition);
            }
        }
        return partitionOffsetArray;
    }

    /**
     * Get {@code Map<TopicPartition, OffsetAndMetadata>} map used in committing consumers.
     *
     * @param offsets {@code BArray} of Ballerina {@code PartitionOffset} records
     * @return {@code Map<TopicPartition, OffsetAndMetadata>} created using Ballerina {@code PartitionOffset}
     */
    public static Map<TopicPartition, OffsetAndMetadata> getPartitionToMetadataMap(BArray offsets) {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
        for (int i = 0; i < offsets.size(); i++) {
            BMap offset = (BMap) offsets.get(i);
            int offsetValue = offset.getIntValue(KafkaConstants.ALIAS_OFFSET).intValue();
            TopicPartition topicPartition = createTopicPartitionFromPartitionOffset(offset);
            partitionToMetadataMap.put(topicPartition, new OffsetAndMetadata(offsetValue));
        }
        return partitionToMetadataMap;
    }

    /**
     * Get {@code TopicPartition} object from {@code BMap} of Ballerina {@code PartitionOffset}.
     *
     * @param offset BMap consists of Ballerina PartitionOffset record.
     * @return TopicPartition Object created
     */
    public static TopicPartition createTopicPartitionFromPartitionOffset(BMap offset) {
        BMap partition = (BMap) offset.get(KafkaConstants.ALIAS_PARTITION);
        String topic = partition.getStringValue(KafkaConstants.ALIAS_TOPIC).getValue();
        int partitionValue = partition.getIntValue(KafkaConstants.ALIAS_PARTITION).intValue();

        return new TopicPartition(topic, partitionValue);
    }

    /**
     * Get the Integer value from an Object, if possible.
     *
     * @param value  the {@code Object} which needs to be converted to int
     * @param name   name of the parameter, for logging purposes
     * @param logger {@code Logger} instance to log if there's an issue
     * @return Integer value of the {@code Object}, {@code null} otherwise
     */
    public static Integer getIntValue(Object value, BString name, Logger logger) {
        Long longValue = getLongValue(value);
        if (Objects.isNull(longValue)) {
            return null;
        }
        return getIntFromLong(longValue, logger, name.getValue());
    }

    /**
     * Get the {@code int} value from a {@code long} value.
     *
     * @param longValue {@code long} value, which we want to convert
     * @param logger    {@code Logger} instance, to log the error if there's an error
     * @param name      parameter name, which will be converted. This is required for logging purposes
     * @return {@code int} value of the {@code long} value, if possible, {@code Integer.MAX_VALUE} is the number is too
     * large
     */
    public static int getIntFromLong(long longValue, Logger logger, String name) {
        try {
            return Math.toIntExact(longValue);
        } catch (ArithmeticException e) {
            logger.warn("The value set for {} needs to be less than {}. The {} value is set to {}", name,
                        Integer.MAX_VALUE, name, Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Get the {@code Long} value from an {@code Object}.
     *
     * @param value Object from which we want to get the Long value
     * @return Long value of the Object, if present. {@code null} otherwise
     */
    public static Long getLongValue(Object value) {
        if (Objects.isNull(value)) {
            return null;
        }
        return (Long) value;
    }

    /**
     * Get the default API timeout defined in the Kafka configurations.
     *
     * @param consumerProperties - Native consumer properties object
     * @return value of the default api timeout, if defined, -1 otherwise.
     */
    public static int getDefaultApiTimeout(Properties consumerProperties) {
        if (Objects.nonNull(consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG))) {
            return (int) consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        }
        return KafkaConstants.DURATION_UNDEFINED_VALUE;
    }

    public static void createKafkaProducer(Properties producerProperties, BObject producerObject) {
        KafkaProducer kafkaProducer = new KafkaProducer<>(producerProperties);
        producerObject.addNativeData(KafkaConstants.NATIVE_PRODUCER, kafkaProducer);
        producerObject.addNativeData(KafkaConstants.NATIVE_PRODUCER_CONFIG, producerProperties);
        producerObject.addNativeData(KafkaConstants.BOOTSTRAP_SERVERS,
                                     producerProperties.getProperty(KafkaConstants.BOOTSTRAP_SERVERS));
        producerObject.addNativeData(KafkaConstants.CLIENT_ID, getClientIdFromProperties(producerProperties));
        KafkaMetricsUtil.reportNewProducer(producerObject);
    }

    public static String getBrokerNames(BObject listener) {
        BMap<BString, Object> listenerConfigurations = listener.getMapValue(
                KafkaConstants.CONSUMER_CONFIG_FIELD_NAME);
        return listenerConfigurations.get(KafkaConstants.CONSUMER_BOOTSTRAP_SERVERS_CONFIG).toString();
    }

    public static String getTopicNamesString(List<String> topicsList) {
        return String.join(", ", topicsList);
    }

    public static String getClientIdFromProperties(Properties properties) {
        if (properties == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        String clientId = properties.getProperty(KafkaConstants.CLIENT_ID);
        if (clientId == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        return clientId;
    }

    public static String getBootstrapServers(BObject object) {
        if (object == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        String bootstrapServers = (String) object.getNativeData(KafkaConstants.BOOTSTRAP_SERVERS);
        if (bootstrapServers == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        return bootstrapServers;
    }

    public static String getClientId(BObject object) {
        if (object == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        String clientId = (String) object.getNativeData(KafkaConstants.CLIENT_ID);
        if (clientId == null) {
            return KafkaObservabilityConstants.UNKNOWN;
        }
        return clientId;
    }

    public static Object invokeMethodSync(Runtime runtime, BObject object, String methodName, String strandName,
                                          StrandMetadata metadata, int timeout, Object... args) {
        Semaphore semaphore = new Semaphore(0);
        final BError[] errorValue = new BError[1];
        Object result = runtime.invokeMethodAsync(object, methodName, strandName, metadata, new Callback() {
            @Override
            public void notifySuccess(Object obj) {
                semaphore.release();
            }

            @Override
            public void notifyFailure(BError error) {
                errorValue[0] = error;
                semaphore.release();
            }
        }, args);
        try {
            semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
        if (errorValue[0] != null) {
            throw errorValue[0];
        }
        return result;
    }
}
