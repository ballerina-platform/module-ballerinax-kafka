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

package io.ballerina.stdlib.kafka.nativeimpl.producer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_PARTITION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_TOPIC;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_PRODUCER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.OFFSET_FIELD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.RECORD_METADATA_TYPE_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERIALIZED_KEY_SIZE_FIELD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERIALIZED_VALUE_SIZE_FIELD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.TIMESTAMP_FIELD;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.ModuleUtils.getModule;
import static io.ballerina.stdlib.kafka.utils.TransactionUtils.handleTransactions;

/**
 * Native method to send different types of keys and values to kafka broker from ballerina kafka producer.
 */
public class Send {

    private static final Logger logger = LoggerFactory.getLogger(Send.class);

    public static Object sendExternal(Environment env, BObject producer, BArray value, BString topic,
                                      BArray headerList, Object key, Object partition, Object timestamp) {
        ProducerRecord<?, byte[]> record = getProducerRecord(value, topic, headerList, key, partition, timestamp);
        KafkaTracingUtil.traceResourceInvocation(env, producer, record.topic());
        final CompletableFuture<Object> balFuture = new CompletableFuture<>();
        KafkaProducer kafkaProducer = (KafkaProducer) producer.getNativeData(NATIVE_PRODUCER);
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            handleTransactions(producer);
        }
        Thread.startVirtualThread(() -> {
            try {
                kafkaProducer.send(record, (metadata, e) -> {
                    if (Objects.nonNull(e)) {
                        KafkaMetricsUtil.reportProducerError(producer,
                                KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                        String detailedError = getDetailedErrorMessage(e);
                        logger.error("Failed to send data to Kafka server for topic '{}': {}",
                                    record.topic(), detailedError, e);
                        balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + detailedError));
                    } else {
                        KafkaMetricsUtil.reportPublish(producer, record.topic(), record.value());
                        balFuture.complete(null);
                    }
                });
            } catch (IllegalStateException | KafkaException e) {
                KafkaMetricsUtil.reportProducerError(producer, KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                String detailedError = getDetailedErrorMessage(e);
                logger.error("Failed to send data to Kafka server for topic '{}': {}",
                            record.topic(), detailedError, e);
                balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + detailedError));
            }
        });
        return ModuleUtils.getResult(balFuture);
    }

    public static Object sendWithMetadataExternal(Environment env, BObject producer, BArray value, BString topic,
                                                  BArray headerList, Object key, Object partition, Object timestamp) {
        ProducerRecord<?, byte[]> record = getProducerRecord(value, topic, headerList, key, partition, timestamp);
        KafkaTracingUtil.traceResourceInvocation(env, producer, record.topic());
        final CompletableFuture<Object> balFuture = new CompletableFuture<>();
        KafkaProducer kafkaProducer = (KafkaProducer) producer.getNativeData(NATIVE_PRODUCER);
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            handleTransactions(producer);
        }
        Thread.startVirtualThread(() -> {
            try {
                Future<RecordMetadata> metadataFuture = kafkaProducer.send(record);
                RecordMetadata metadata = metadataFuture.get();
                balFuture.complete(populateRecordMetadata(metadata));
            } catch (Exception e) {
                KafkaMetricsUtil.reportProducerError(producer, KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                String detailedError = getDetailedErrorMessage(e);
                logger.error("Failed to send data to Kafka server for topic '{}': {}",
                            record.topic(), detailedError, e);
                balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + detailedError));
            }
        });
        return ModuleUtils.getResult(balFuture);
    }

    private static List<Header> getHeadersFromBHeaders(BArray headerList) {
        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < headerList.size(); i++) {
            BArray headerItem = (BArray) headerList.get(i);
            headers.add(new RecordHeader(headerItem.getBString(0).getValue(),
                    ((BArray) headerItem.get(1)).getByteArray()));
        }
        return headers;
    }

    private static ProducerRecord<?, byte[]> getProducerRecord(BArray value, BString topic, BArray headerList,
                                                               Object key, Object partition, Object timestamp) {
        Integer partitionValue = partition instanceof Long ? ((Long) partition).intValue() : null;
        Long timestampValue = timestamp instanceof Long ? (Long) timestamp : null;
        List<Header> headers = getHeadersFromBHeaders(headerList);
        byte[] keyBytes = null;
        if (key != null) {
            // This can't be anything else since the key is defined as `byte[]?` in the ballerina
            keyBytes = ((BArray) key).getBytes();
        }
        return new ProducerRecord<>(topic.getValue(), partitionValue, timestampValue, keyBytes, value.getBytes(),
                headers);
    }

    private static BMap<BString, Object> populateRecordMetadata(RecordMetadata metadata) {
        BMap<BString, Object> valueMap = ValueCreator.createMapValue();
        valueMap.put(ALIAS_TOPIC, StringUtils.fromString(metadata.topic()));
        valueMap.put(ALIAS_PARTITION, (long) metadata.partition());
        valueMap.put(SERIALIZED_KEY_SIZE_FIELD, (long) metadata.serializedKeySize());
        valueMap.put(SERIALIZED_VALUE_SIZE_FIELD, (long) metadata.serializedValueSize());
        if (metadata.hasOffset()) {
            valueMap.put(OFFSET_FIELD, metadata.offset());
        }
        if (metadata.hasTimestamp()) {
            valueMap.put(TIMESTAMP_FIELD, metadata.timestamp());
        }
        return ValueCreator.createRecordValue(getModule(), RECORD_METADATA_TYPE_NAME, valueMap);
    }

    /**
     * Extracts detailed error message from exceptions during send operations.
     * Provides specific guidance for SSL, authentication, and connection errors.
     *
     * @param e The caught exception.
     * @return Detailed error message with cause information and troubleshooting guidance.
     */
    private static String getDetailedErrorMessage(Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        String message = cause.getMessage() != null ? cause.getMessage() : e.getMessage();
        String causeClass = cause.getClass().getSimpleName();

        // Provide specific guidance for common error types
        if (message.contains("SSL") || message.contains("ssl") || causeClass.contains("SSL")) {
            return message + ". SSL/TLS error occurred. Please verify: " +
                   "1) Certificate paths are correct, " +
                   "2) Truststore/keystore are accessible and valid, " +
                   "3) Certificates are not expired, " +
                   "4) SSL protocol versions match broker configuration.";
        } else if (message.contains("SaslAuthentication") || message.contains("Authentication failed") ||
                   message.contains("SASL") || message.contains("authentication") || causeClass.contains("Sasl")) {
            return message + ". SASL authentication error occurred. Please verify: " +
                   "1) Username and password are correct, " +
                   "2) Authentication mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) matches broker configuration, " +
                   "3) User has necessary permissions on the broker.";
        } else if (message.contains("TimeoutException") || message.contains("timeout") ||
                   causeClass.contains("Timeout")) {
            return message + ". Connection timeout occurred. Please verify: " +
                   "1) Bootstrap servers configuration is correct, " +
                   "2) Kafka brokers are running and accessible, " +
                   "3) Network connectivity and firewall rules allow connection, " +
                   "4) Consider increasing timeout values if network latency is high.";
        } else if (message.contains("UnknownHostException") || message.contains("nodename nor servname provided") ||
                   causeClass.contains("UnknownHost")) {
            return message + ". Cannot resolve broker hostname. Please verify: " +
                   "1) Bootstrap servers hostnames are spelled correctly, " +
                   "2) DNS resolution is working properly, " +
                   "3) Hostnames are reachable from this network.";
        } else if (message.contains("Connection refused") || message.contains("Connection reset") ||
                   message.contains("ConnectionException") || causeClass.contains("Connection")) {
            return message + ". Connection to broker failed. Please verify: " +
                   "1) Kafka brokers are running, " +
                   "2) Port numbers are correct in bootstrap servers, " +
                   "3) Network route to brokers is available, " +
                   "4) Firewall is not blocking the connection.";
        } else if (message.contains("NotLeaderForPartitionException") || message.contains("LeaderNotAvailable")) {
            return message + ". Kafka broker leadership issue. This may be temporary during broker restart or " +
                   "leader election. Retry the operation or verify cluster health.";
        } else if (message.contains("RecordTooLargeException") || message.contains("too large")) {
            return message + ". Message size exceeds broker limits. Please verify: " +
                   "1) Message size is within broker's max.request.size limit, " +
                   "2) Topic's max.message.bytes configuration, " +
                   "3) Consider compressing messages or splitting large payloads.";
        } else if (message.contains("AuthorizationException") || message.contains("authorization")) {
            return message + ". Authorization error occurred. Please verify: " +
                   "1) User has WRITE permissions on the topic, " +
                   "2) ACLs are correctly configured on the broker, " +
                   "3) User is authorized for the requested operation.";
        } else {
            return message;
        }
    }
}
