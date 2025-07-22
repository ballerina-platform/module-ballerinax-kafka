/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.flags.TypeFlags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_DURATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_PARTITION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_TOPIC;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.DURATION_UNDEFINED_VALUE;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.OFFSET_AND_TIMESTAMP_TYPE_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.TOPIC_PARTITION_TYPE_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getDefaultApiTimeout;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntFromBDecimal;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntFromLong;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getPartitionOffsetArrayFromOffsetMap;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartition;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionList;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populateOffsetAndTimestampRecord;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populatePartitionOffsetRecord;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populateTopicPartitionRecord;
import static io.ballerina.stdlib.kafka.utils.ModuleUtils.getModule;

/**
 * Native methods to get different offset values for the ballerina kafka consumer.
 */
public class GetOffsets {
    private static final Logger logger = LoggerFactory.getLogger(GetOffsets.class);

    private static final Type topicParitionType = TypeCreator.createRecordType(TOPIC_PARTITION_TYPE_NAME, getModule(),
            0, false, TypeFlags.ANYDATA);
    private static Type offsetAndTimestampType = TypeCreator.createRecordType(OFFSET_AND_TIMESTAMP_TYPE_NAME,
            getModule(), 0, false, TypeFlags.ANYDATA);
    private static final Type offsetAndTimestampNilableType = TypeCreator.createUnionType(offsetAndTimestampType,
            PredefinedTypes.TYPE_NULL);
    private static TupleType topicPartitionOffsetTupleType = TypeCreator.createTupleType(List.of(topicParitionType,
            offsetAndTimestampNilableType));

    /**
     * Returns the beginning offsets of given topic partitions for the ballerina kafka consumer.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic partition array to get the beginning offsets.
     * @param duration        Duration in milliseconds to try the operation.
     * @return ballerina {@code PartitionOffset} array or @{BError} if an error occurred.
     */
    public static Object getBeginningOffsets(Environment environment, BObject consumerObject,
                                             BArray topicPartitions, BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        List<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);
        Map<TopicPartition, Long> offsetMap;
        try {
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetMap = getBeginningOffsetsWithDuration(kafkaConsumer, partitionList, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetMap = getBeginningOffsetsWithDuration(kafkaConsumer, partitionList, defaultApiTimeout);
                } else {
                    offsetMap = kafkaConsumer.beginningOffsets(partitionList);
                }
            }
            return getPartitionOffsetArrayFromOffsetMap(offsetMap);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                    KafkaObservabilityConstants.ERROR_TYPE_GET_BEG_OFFSETS);
            return createKafkaError("Failed to retrieve offsets for the topic partitions: " + e.getMessage());
        }
    }

    /**
     * Returns the committed offsets for the given topic partition for the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topicPartition Topic partition record to get the offsets.
     * @param duration       Duration in milliseconds to try the operation.
     * @return ballerina {@code PartitionOffset} value or @{BError} if an error occurred.
     */
    public static Object getCommittedOffset(Environment environment, BObject consumerObject, BMap<BString,
            Object> topicPartition, BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        String topic = topicPartition.getStringValue(ALIAS_TOPIC).getValue();
        Long partition = topicPartition.getIntValue(ALIAS_PARTITION);
        TopicPartition tp = new TopicPartition(topic, getIntFromLong(partition, logger, ALIAS_PARTITION.getValue()));

        try {
            OffsetAndMetadata offsetAndMetadata;
            BMap<BString, Object> offset;
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetAndMetadata = getOffsetAndMetadataWithDuration(kafkaConsumer, tp, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetAndMetadata = getOffsetAndMetadataWithDuration(kafkaConsumer, tp, defaultApiTimeout);
                } else {
                    offsetAndMetadata = kafkaConsumer.committed(tp);
                }
            }
            if (Objects.isNull(offsetAndMetadata)) {
                return null;
            }
            offset = populatePartitionOffsetRecord(topicPartition, offsetAndMetadata.offset());
            return offset;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                    KafkaObservabilityConstants.ERROR_TYPE_GET_COMMIT_OFFSET);
            return createKafkaError("Failed to retrieve committed offsets: " + e.getMessage());
        }
    }

    /**
     * Returns the end offsets for given array of topic partitions for the ballerina kafka consumer.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic partition array to get the end offsets.
     * @param duration        Duration in milliseconds to try the operation.
     * @return ballerina {@code PartitionOffset} array or @{BError} if an error occurred.
     */
    public static Object getEndOffsets(Environment environment, BObject consumerObject, BArray topicPartitions,
                                       BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        ArrayList<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);
        Map<TopicPartition, Long> offsetMap;

        try {
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetMap = getEndOffsetsWithDuration(kafkaConsumer, partitionList, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    offsetMap = getEndOffsetsWithDuration(kafkaConsumer, partitionList, defaultApiTimeout);
                } else {
                    offsetMap = kafkaConsumer.endOffsets(partitionList);
                }
            }
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                    KafkaObservabilityConstants.ERROR_TYPE_GET_END_OFFSETS);
            return createKafkaError("Failed to retrieve end offsets for the consumer: " + e.getMessage());
        }

        return getPartitionOffsetArrayFromOffsetMap(offsetMap);
    }

    /**
     * Returns the position offset of a given topic partition for the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topicPartition Topic partition of which the position offset needed.
     * @param duration       Duration in milliseconds to try the operation.
     * @return ballerina {@code PartitionOffset} value or @{BError} if an error occurred.
     */
    public static Object getPositionOffset(Environment environment, BObject consumerObject, BMap<BString,
            Object> topicPartition, BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        String topic = topicPartition.getStringValue(ALIAS_TOPIC).getValue();
        Long partition = topicPartition.getIntValue(ALIAS_PARTITION);
        TopicPartition tp = new TopicPartition(topic, getIntFromLong(partition, logger, ALIAS_PARTITION.getValue()));

        try {
            long position;
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    position = getPositionWithDuration(kafkaConsumer, tp, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    position = getPositionWithDuration(kafkaConsumer, tp, defaultApiTimeout);
                } else {
                    position = kafkaConsumer.position(tp);
                }
            }
            return position;
        } catch (IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                    KafkaObservabilityConstants.ERROR_TYPE_GET_POSITION_OFFSET);
            return createKafkaError("Failed to retrieve position offset: " + e.getMessage());
        }
    }

    public static Object offsetsForTimes(Environment environment, BObject consumer,
                                         BArray topicPartitionTimestamps, Object duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumer);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumer.getNativeData(NATIVE_CONSUMER);
        Map<TopicPartition, Long> topicPartitionTimestampsMap =
                getTopicPartitionTimestampsMap(topicPartitionTimestamps);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes;
        try {
            if (duration instanceof BDecimal durationValue) {
                int apiTimeout = getIntFromBDecimal(durationValue, logger, ALIAS_DURATION);
                if (apiTimeout < 0) {
                    return createKafkaError("Invalid duration value: " + duration);
                }
                Duration timeoutDuration = Duration.ofMillis(apiTimeout);
                offsetsForTimes = kafkaConsumer.offsetsForTimes(topicPartitionTimestampsMap, timeoutDuration);
            } else {
                offsetsForTimes = kafkaConsumer.offsetsForTimes(topicPartitionTimestampsMap);
            }
            ArrayType offsetAndTimestampArrayType = TypeCreator.createArrayType(topicPartitionOffsetTupleType);
            BArray offsetAndTimestampArray = ValueCreator.createArrayValue(offsetAndTimestampArrayType);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
                BArray offsetForTimes = getOffsetForTimes(topicPartition, offsetAndTimestamp);
                offsetAndTimestampArray.append(offsetForTimes);
            }
            return offsetAndTimestampArray;
        } catch (Exception e) {
            KafkaMetricsUtil.reportConsumerError(consumer, KafkaObservabilityConstants.ERROR_TYPE_OFFSETS_FOR_TIMES);
            logger.error("Failed to retrieve offsets for times: {}", e.getMessage(), e);
            return createKafkaError("Failed to retrieve offsets for times: ", e);
        }
    }

    private static Map<TopicPartition, Long> getTopicPartitionTimestampsMap(BArray topicPartitionTimestamps) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (int i = 0; i < topicPartitionTimestamps.size(); i++) {
            BArray topicPartitionTimestamp = (BArray) topicPartitionTimestamps.get(i);
            TopicPartition topicPartition = getTopicPartition((BMap<BString, Object>) topicPartitionTimestamp.get(0),
                    logger);
            Long timestamp = topicPartitionTimestamp.getInt(1);
            result.put(topicPartition, timestamp);
        }
        return result;
    }

    private static BArray getOffsetForTimes(TopicPartition topicPartition, OffsetAndTimestamp offsetAndTimestamp) {
        BArray offsetForTimes = ValueCreator.createTupleValue(topicPartitionOffsetTupleType);
        BMap<BString, Object> topicPartitionRecord = populateTopicPartitionRecord(topicPartition.topic(),
                topicPartition.partition());
        offsetForTimes.add(0, topicPartitionRecord);
        if (offsetAndTimestamp != null) {
            BMap<BString, Object> offsetAndTimestampRecord = populateOffsetAndTimestampRecord(offsetAndTimestamp);
            offsetForTimes.add(1, offsetAndTimestampRecord);
        }
        return offsetForTimes;
    }

    private static Map<TopicPartition, Long> getBeginningOffsetsWithDuration(KafkaConsumer consumer,
                                                                             List<TopicPartition> partitions,
                                                                             long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        return consumer.beginningOffsets(partitions, duration);
    }

    private static OffsetAndMetadata getOffsetAndMetadataWithDuration(KafkaConsumer kafkaConsumer,
                                                                      TopicPartition topicPartition, long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        HashSet<TopicPartition> topicPartitionSet = new HashSet<>();
        topicPartitionSet.add(topicPartition);
        return (OffsetAndMetadata) kafkaConsumer.committed(topicPartitionSet, duration).get(topicPartition);
    }

    private static Map<TopicPartition, Long> getEndOffsetsWithDuration(KafkaConsumer consumer,
                                                                       ArrayList<TopicPartition> partitions,
                                                                       long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        return consumer.endOffsets(partitions, duration);
    }

    private static long getPositionWithDuration(KafkaConsumer kafkaConsumer, TopicPartition topicPartition,
                                                long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        return kafkaConsumer.position(topicPartition, duration);
    }
}
