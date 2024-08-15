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
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_DURATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.DURATION_UNDEFINED_VALUE;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.UNCHECKED;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getDefaultApiTimeout;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntFromBDecimal;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionList;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionRecord;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populateTopicPartitionRecord;

/**
 * Native methods to handle ballerina kafka consumer subscriptions.
 */
public class ConsumerInformationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerInformationHandler.class);
    private static final ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);

    /**
     * Assign ballerina kafka consumer to the given topic array.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic partition array to which the consumer need to be subscribed.
     * @return {@code BError}, if there's an error, null otherwise.
     */
    public static Object assign(Environment environment, BObject consumerObject, BArray topicPartitions) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        List<TopicPartition> partitions = getTopicPartitionList(topicPartitions, logger);
        try {
            synchronized (kafkaConsumer) {
                kafkaConsumer.assign(partitions);
            }
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_ASSIGN);
            return createKafkaError("Failed to assign topics for the consumer: " + e.getMessage());
        }
        return null;
    }

    /**
     * Get the current assignment of the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return Topic partition ballerina array. If there's an error in the process {@code BError} is returned.
     */
    public static Object getAssignment(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        BArray topicPartitionArray =
                ValueCreator.createArrayValue(TypeCreator.createArrayType(getTopicPartitionRecord().getType()));
        try {
            Set<TopicPartition> topicPartitions;
            synchronized (kafkaConsumer) {
                topicPartitions = kafkaConsumer.assignment();
            }
            for (TopicPartition partition : topicPartitions) {
                BMap<BString, Object> tp = populateTopicPartitionRecord(partition.topic(), partition.partition());
                topicPartitionArray.append(tp);
            }
            return topicPartitionArray;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_GET_ASSIGNMENT);
            return createKafkaError("Failed to retrieve assignment for the consumer: " + e.getMessage());
        }
    }

    /**
     * Get the available topics from the kafka broker for the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param duration       Duration in milliseconds to try the operation.
     * @return Array of ballerina strings, which consists of the available topics.
     */
    public static Object getAvailableTopics(Environment environment, BObject consumerObject, BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        Map<String, List<PartitionInfo>> topics;
        try {
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    topics = getAvailableTopicWithDuration(kafkaConsumer, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    topics = getAvailableTopicWithDuration(kafkaConsumer, defaultApiTimeout);
                } else {
                    topics = kafkaConsumer.listTopics();
                }
            }
            return getBArrayFromMap(topics);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_GET_TOPICS);
            return createKafkaError("Failed to retrieve available topics: " + e.getMessage());
        }
    }

    /**
     * Get the currently paused partitions for given consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return Array of ballerina strings, which consists of the paused topics.
     */
    public static Object getPausedPartitions(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        BArray topicPartitionArray =
                ValueCreator.createArrayValue(TypeCreator.createArrayType(getTopicPartitionRecord().getType()));
        try {
            Set<TopicPartition> pausedPartitions;
            synchronized (kafkaConsumer) {
                pausedPartitions = kafkaConsumer.paused();
            }
            for (TopicPartition partition : pausedPartitions) {
                BMap<BString, Object> tp = populateTopicPartitionRecord(partition.topic(), partition.partition());
                topicPartitionArray.append(tp);
            }
            return topicPartitionArray;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_GET_PAUSED_PARTITIONS);
            return createKafkaError("Failed to retrieve paused partitions: " + e.getMessage());
        }
    }

    /**
     * Get the topic partition data for the given topic.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topic          Topic, of which the data is needed.
     * @param duration       Duration in milliseconds to try the operation.
     * @return Topic partition array of the given topic.
     */
    public static Object getTopicPartitions(Environment environment, BObject consumerObject, BString topic,
                                            BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);

        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);

        try {
            List<PartitionInfo> partitionInfoList;
            synchronized (kafkaConsumer) {
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    partitionInfoList = getPartitionInfoList(kafkaConsumer, topic.getValue(), apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    partitionInfoList = getPartitionInfoList(kafkaConsumer, topic.getValue(), defaultApiTimeout);
                } else {
                    partitionInfoList = kafkaConsumer.partitionsFor(topic.getValue());
                }
            }
            BArray topicPartitionArray =
                    ValueCreator.createArrayValue(TypeCreator.createArrayType(getTopicPartitionRecord().getType()));
            for (PartitionInfo info : partitionInfoList) {
                BMap<BString, Object> partition = populateTopicPartitionRecord(info.topic(), info.partition());
                topicPartitionArray.append(partition);
            }
            return topicPartitionArray;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_GET_TOPIC_PARTITIONS);
            return createKafkaError("Failed to retrieve topic partitions for the consumer: "
                                                       + e.getMessage());
        }
    }

    /**
     * Get the currently subscribed topics of the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return Array of ballerina strings, which consists of the subscribed topics.
     */
    public static Object getSubscription(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);

        try {
            Set<String> subscriptions;
            synchronized (kafkaConsumer) {
                subscriptions = kafkaConsumer.subscription();
            }
            BArray arrayValue = ValueCreator.createArrayValue(stringArrayType);
            if (!subscriptions.isEmpty()) {
                for (String subscription : subscriptions) {
                    arrayValue.append(StringUtils.fromString(subscription));
                }
            }
            return arrayValue;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_GET_SUBSCRIPTION);
            return createKafkaError("Failed to retrieve subscribed topics: " + e.getMessage());
        }
    }

    private static Map<String, List<PartitionInfo>> getAvailableTopicWithDuration(KafkaConsumer kafkaConsumer,
                                                                                  long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        return kafkaConsumer.listTopics(duration);
    }

    private static BArray getBArrayFromMap(Map<String, List<PartitionInfo>> map) {
        BArray bArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING));
        if (!map.keySet().isEmpty()) {
            for (String topic : map.keySet()) {
                bArray.append(StringUtils.fromString(topic));
            }
        }
        return bArray;
    }

    @SuppressWarnings(UNCHECKED)
    private static List<PartitionInfo> getPartitionInfoList(KafkaConsumer kafkaConsumer, String topic, long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        return (List<PartitionInfo>) kafkaConsumer.partitionsFor(topic, duration);
    }
}
