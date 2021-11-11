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

package io.ballerina.stdlib.kafka.impl;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.async.StrandMetadata;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.stdlib.kafka.api.KafkaListener;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaObserverContext;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAttachedFunctionReturnType;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getResourceParameters;

/**
 * Kafka Connector Consumer for Ballerina.
 */
public class KafkaListenerImpl implements KafkaListener {

    private final BObject service;
    private final BObject listener;
    private final Runtime bRuntime;

    public KafkaListenerImpl(BObject listener, BObject service, Runtime bRuntime) {
        this.bRuntime = bRuntime;
        this.listener = listener;
        this.service = service;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRecordsReceived(ConsumerRecords records, KafkaConsumer kafkaConsumer, String groupId,
                                  KafkaPollCycleFutureListener consumer) {
        listener.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        executeResource(listener, consumer, records);
        KafkaMetricsUtil.reportConsume(listener, records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable throwable) {
        KafkaMetricsUtil.reportConsumerError(listener, KafkaObservabilityConstants.ERROR_TYPE_MSG_RECEIVED);
    }

    private void executeResource(BObject listener, KafkaPollCycleFutureListener consumer, ConsumerRecords records) {
        StrandMetadata metadata = new StrandMetadata(ModuleUtils.getModule().getOrg(),
                                                     ModuleUtils.getModule().getName(),
                                                     ModuleUtils.getModule().getVersion(), KAFKA_RESOURCE_ON_RECORD);
        if (ObserveUtils.isTracingEnabled()) {
            Type returnType = getAttachedFunctionReturnType(service, KAFKA_RESOURCE_ON_RECORD, 2);
            Map<String, Object> properties = getNewObserverContextInProperties(listener);
            if (service.getType().isIsolated(KAFKA_RESOURCE_ON_RECORD)) {
                bRuntime.invokeMethodAsyncConcurrently(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                        consumer, properties, returnType, getResourceParameters(service, this.listener, records));
            } else {
                bRuntime.invokeMethodAsyncSequentially(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                        consumer, properties, returnType, getResourceParameters(service, this.listener, records));
            }
        } else {
            if (service.getType().isIsolated(KAFKA_RESOURCE_ON_RECORD)) {
                bRuntime.invokeMethodAsyncConcurrently(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                        consumer, null, PredefinedTypes.TYPE_NULL,
                        getResourceParameters(service, this.listener, records));
            } else {
                bRuntime.invokeMethodAsyncSequentially(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                        consumer, null, PredefinedTypes.TYPE_NULL,
                        getResourceParameters(service, this.listener, records));
            }
        }
    }

    private Map<String, Object> getNewObserverContextInProperties(BObject listener) {
        Map<String, Object> properties = new HashMap<>();
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(listener),
                                                                        KafkaUtils.getBootstrapServers(listener));
        properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
        return properties;
    }
}
