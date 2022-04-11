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
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.MethodType;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.stdlib.kafka.api.KafkaListener;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaObserverContext;
import io.ballerina.stdlib.kafka.utils.KafkaConstants;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static io.ballerina.runtime.api.TypeTags.ARRAY_TAG;
import static io.ballerina.runtime.api.TypeTags.INTERSECTION_TAG;
import static io.ballerina.runtime.api.TypeTags.OBJECT_TYPE_TAG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_ERROR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAttachedFunctionReturnType;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populateConsumerRecord;

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
        executeOnError(throwable);
    }

    private void executeResource(BObject listener, KafkaPollCycleFutureListener consumer, ConsumerRecords records) {
        StrandMetadata metadata = new StrandMetadata(ModuleUtils.getModule().getOrg(),
                ModuleUtils.getModule().getName(), ModuleUtils.getModule().getMajorVersion(), KAFKA_RESOURCE_ON_RECORD);
        Map<String, Object> properties = null;
        Type returnType = null;
        if (ObserveUtils.isTracingEnabled()) {
            properties = getNewObserverContextInProperties(listener);
            returnType = getAttachedFunctionReturnType(service, KAFKA_RESOURCE_ON_RECORD);
        }
        if (service.getType().isIsolated() && service.getType().isIsolated(KAFKA_RESOURCE_ON_RECORD)) {
            bRuntime.invokeMethodAsyncConcurrently(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                    consumer, properties, returnType == null ? PredefinedTypes.TYPE_NULL : returnType,
                    getResourceParameters(service, this.listener, records));
        } else {
            bRuntime.invokeMethodAsyncSequentially(service, KAFKA_RESOURCE_ON_RECORD, null, metadata,
                    consumer, properties, returnType == null ? PredefinedTypes.TYPE_NULL : returnType,
                    getResourceParameters(service, this.listener, records));
        }
    }

    private void executeOnError(Throwable throwable) {
        StrandMetadata metadata = new StrandMetadata(ModuleUtils.getModule().getOrg(),
                ModuleUtils.getModule().getName(), ModuleUtils.getModule().getMajorVersion(), KAFKA_RESOURCE_ON_ERROR);
        Map<String, Object> properties = null;
        if (ObserveUtils.isTracingEnabled()) {
            properties = getNewObserverContextInProperties(listener);
        }
        if (service.getType().isIsolated() && service.getType().isIsolated(KAFKA_RESOURCE_ON_ERROR)) {
            bRuntime.invokeMethodAsyncConcurrently(service, KAFKA_RESOURCE_ON_ERROR, null, metadata,
                    null, properties, PredefinedTypes.TYPE_NULL,
                    KafkaUtils.createKafkaError(throwable.getMessage()), true);
        } else {
            bRuntime.invokeMethodAsyncSequentially(service, KAFKA_RESOURCE_ON_ERROR, null, metadata,
                    null, properties, PredefinedTypes.TYPE_NULL,
                    KafkaUtils.createKafkaError(throwable.getMessage()), true);
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

    public Object[] getResourceParameters(BObject service, BObject listener, ConsumerRecords records) {
        Parameter[] parameters = getOnConsumerRecordMethod(service).get().getParameters();
        boolean callerExists = false;
        boolean consumerRecordsExists = false;
        Object[] arguments = new Object[parameters.length * 2];
        int index = 0;
        for (Parameter parameter : parameters) {
            switch (parameter.type.getTag()) {
                case OBJECT_TYPE_TAG:
                    if (callerExists) {
                        throw KafkaUtils.createKafkaError("Invalid remote function signature");
                    }
                    callerExists = true;
                    arguments[index++] = createCaller(listener);
                    arguments[index++] = true;
                    break;
                case INTERSECTION_TAG:
                case ARRAY_TAG:
                    if (consumerRecordsExists) {
                        throw KafkaUtils.createKafkaError("Invalid remote function signature");
                    }
                    consumerRecordsExists = true;
                    arguments[index++] = getConsumerRecords(records, parameter);
                    arguments[index++] = true;
                    break;
                default:
                    throw KafkaUtils.createKafkaError("Invalid remote function signature");
            }
        }
        return arguments;
    }

    private BObject createCaller(BObject listener) {
        BObject caller = ValueCreator.createObjectValue(ModuleUtils.getModule(), KafkaConstants.CALLER_STRUCT_NAME);
        KafkaConsumer consumer = (KafkaConsumer) listener.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) listener.getNativeData(NATIVE_CONSUMER_CONFIG);
        caller.addNativeData(NATIVE_CONSUMER, consumer);
        caller.addNativeData(NATIVE_CONSUMER_CONFIG, consumerProperties);
        return caller;
    }

    private BArray getConsumerRecords(ConsumerRecords records, Parameter parameter) {
        RecordType recordType = (RecordType) ((ArrayType) parameter.type).getElementType();
        List<BMap<BString, Object>> recordMapList = new ArrayList();
        for (Object record : records) {
            BMap<BString, Object> consumerRecord = populateConsumerRecord((ConsumerRecord) record, recordType);
            recordMapList.add(consumerRecord);
        }
        BArray consumerRecordsArray = ValueCreator.createArrayValue(recordMapList.toArray(),
                TypeCreator.createArrayType(recordType, parameter.type.isReadOnly()));
        return consumerRecordsArray;
    }

    private static Optional<MethodType> getOnConsumerRecordMethod(BObject service) {
        MethodType[] methodTypes = service.getType().getMethods();
        return Stream.of(methodTypes)
                .filter(methodType -> KAFKA_RESOURCE_ON_RECORD.equals(methodType.getName())).findFirst();
    }
}
