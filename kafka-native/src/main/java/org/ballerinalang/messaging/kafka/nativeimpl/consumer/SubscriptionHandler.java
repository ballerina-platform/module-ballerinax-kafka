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

package org.ballerinalang.messaging.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.messaging.kafka.observability.KafkaMetricsUtil;
import org.ballerinalang.messaging.kafka.observability.KafkaObservabilityConstants;
import org.ballerinalang.messaging.kafka.observability.KafkaTracingUtil;
import org.ballerinalang.messaging.kafka.utils.KafkaConstants;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.createKafkaError;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getStringListFromStringBArray;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getTopicNamesString;

/**
 * Native methods to handle subscription of the ballerina kafka consumer.
 */
public class SubscriptionHandler {
    private static final PrintStream console = System.out;

    /**
     * Subscribe the ballerina kafka consumer to the given array of topics.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topics         Ballerina {@code string[]} of topics.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object subscribe(Environment environment, BObject consumerObject, BArray topics) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        List<String> topicsList = getStringListFromStringBArray(topics);
        try {
            kafkaConsumer.subscribe(topicsList);
            Set<String> subscribedTopics = kafkaConsumer.subscription();
            KafkaMetricsUtil.reportBulkSubscription(consumerObject, subscribedTopics);
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_SUBSCRIBE);
            return createKafkaError("Failed to subscribe to the provided topics: " + e.getMessage());
        }
        console.println(KafkaConstants.SUBSCRIBED_TOPICS + getTopicNamesString(topicsList));
        return null;
    }

    /**
     * Subscribes the ballerina kafka consumer to the topics matching the given regex.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topicRegex     Regex to match topics to subscribe.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object subscribeToPattern(Environment environment, BObject consumerObject, BString topicRegex) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        try {
            kafkaConsumer.subscribe(Pattern.compile(topicRegex.getValue()));
            // TODO: This sometimes not updating since Kafka not updates the subscription tight away
            Set<String> topicsList = kafkaConsumer.subscription();
            KafkaMetricsUtil.reportBulkSubscription(consumerObject, topicsList);
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                    KafkaObservabilityConstants.ERROR_TYPE_SUBSCRIBE_PATTERN);
            return createKafkaError("Failed to subscribe to the topics: " + e.getMessage());
        }
        return null;
    }

    /**
     * Subscribes the ballerina kafka consumer and re-balances the assignments.
     *
     * @param consumerObject       Kafka consumer object from ballerina.
     * @param topics               Ballerina {@code string[]} of topics.
     * @return {@code BError}, if there's any error, null otherwise.
     */
//    public static Object subscribeWithPartitionRebalance(Environment env, BObject consumerObject, BArray topics,
//                                                         BFunctionPointer onPartitionsRevoked,
//                                                         BFunctionPointer onPartitionsAssigned) {
//        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
//        Future balFuture = env.markAsync();
//        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
//        List<String> topicsList = getStringListFromStringBArray(topics);
//        ConsumerRebalanceListener consumer = new SubscriptionHandler.KafkaRebalanceListener(env,
//                                                                                            consumerObject);
//        try {
//            kafkaConsumer.subscribe(topicsList, consumer);
//            Set<String> subscribedTopics = kafkaConsumer.subscription();
//            KafkaMetricsUtil.reportBulkSubscription(consumerObject, subscribedTopics);
//            balFuture.complete(null);
//        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
//            KafkaMetricsUtil.reportConsumerError(consumerObject,
//                                         KafkaObservabilityConstants.ERROR_TYPE_SUBSCRIBE_PARTITION_REBALANCE);
//            balFuture.complete(createKafkaError("Failed to subscribe the consumer: " + e.getMessage()));
//        }
//        return null;
//    }

    /**
     * Unsubscribe the ballerina kafka consumer from all the topics.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object unsubscribe(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        try {
            Set<String> topics = kafkaConsumer.subscription();
            kafkaConsumer.unsubscribe();
            KafkaMetricsUtil.reportBulkUnsubscription(consumerObject, topics);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_UNSUBSCRIBE);
            return createKafkaError("Failed to unsubscribe the consumer: " + e.getMessage());
        }
        return null;
    }

    /**
     * Implementation for {@link ConsumerRebalanceListener} interface from connector side. We register this listener at
     * subscription.
     * <p>
     * {@inheritDoc}
     */
//    static class KafkaRebalanceListener implements ConsumerRebalanceListener {
//
//        private BObject consumer;
//        private Runtime runtime;
//
//        KafkaRebalanceListener(Environment environment, BObject consumer) {
//            this.consumer = consumer;
//            this.runtime = environment.getRuntime();
//
//        }
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//            Object[] inputArgs = {consumer, true, getPartitionsArray(partitions), true};
//            StrandMetadata metadata = new StrandMetadata(ModuleUtils.getModule().getOrg(),
//                                                         ModuleUtils.getModule().getName(),
//                                                         ModuleUtils.getModule().getVersion(),
//                                                         FUNCTION_ON_PARTITION_REVOKED);
//
//            this.runtime.invokeMethodAsync(consumer, FUNCTION_ON_PARTITION_REVOKED, null,
//                                           metadata, null, inputArgs);
//        }
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//            Object[] inputArgs = {consumer, true, getPartitionsArray(partitions), true};
//            StrandMetadata metadata = new StrandMetadata(ModuleUtils.getModule().getOrg(),
//                                                         ModuleUtils.getModule().getName(),
//                                                         ModuleUtils.getModule().getVersion(),
//                                                         FUNCTION_ON_PARTITION_ASSIGNED);
//            this.runtime.invokeMethodAsync(consumer, FUNCTION_ON_PARTITION_ASSIGNED, null,
//                                           metadata, null, inputArgs);
//        }
//
//        private BArray getPartitionsArray(Collection<TopicPartition> partitions) {
//            BArray topicPartitionArray = ValueCreator.createArrayValue(
//                    TypeCreator.createArrayType(getTopicPartitionRecord().getType()));
//            for (TopicPartition partition : partitions) {
//                BMap<BString, Object> topicPartition = populateTopicPartitionRecord(partition.topic(),
//                                                                                        partition.partition());
//                topicPartitionArray.append(topicPartition);
//            }
//            return topicPartitionArray;
//        }
//    }
}
