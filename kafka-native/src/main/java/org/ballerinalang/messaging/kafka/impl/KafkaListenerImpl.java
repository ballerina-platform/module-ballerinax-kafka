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

package org.ballerinalang.messaging.kafka.impl;

import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.async.StrandMetadata;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.messaging.kafka.api.KafkaListener;
import org.ballerinalang.messaging.kafka.observability.KafkaMetricsUtil;
import org.ballerinalang.messaging.kafka.observability.KafkaObservabilityConstants;
import org.ballerinalang.messaging.kafka.observability.KafkaObserverContext;
import org.ballerinalang.messaging.kafka.utils.KafkaUtils;
import org.ballerinalang.messaging.kafka.utils.ModuleUtils;

import java.util.HashMap;
import java.util.Map;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_RECORD;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getAttachedFunctionReturnType;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getResourceParameters;

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
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_RECORD, null, metadata, consumer,
                                       properties, returnType,
                                       getResourceParameters(service, this.listener, records));
        } else {
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_RECORD, null, metadata, consumer,
                                       getResourceParameters(service, this.listener, records));
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
