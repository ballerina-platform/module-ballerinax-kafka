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
                        balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + e.getMessage()));
                    } else {
                        KafkaMetricsUtil.reportPublish(producer, record.topic(), record.value());
                        balFuture.complete(null);
                    }
                });
            } catch (IllegalStateException | KafkaException e) {
                KafkaMetricsUtil.reportProducerError(producer, KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + e.getMessage()));
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
                balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + e.getMessage()));
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
}
