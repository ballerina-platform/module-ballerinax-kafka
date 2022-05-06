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
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.MethodType;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static io.ballerina.runtime.api.TypeTags.ARRAY_TAG;
import static io.ballerina.runtime.api.TypeTags.INTERSECTION_TAG;
import static io.ballerina.runtime.api.TypeTags.OBJECT_TYPE_TAG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_ERROR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PARAM_ANNOTATION_PREFIX;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PARAM_PAYLOAD_ANNOTATION_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.TYPE_CHECKER_OBJECT_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAttachedFunctionReturnType;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getConsumerRecords;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getValueWithIntendedType;

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
        StrandMetadata metadata = getStrandMetadata(KAFKA_RESOURCE_ON_RECORD);
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
        StrandMetadata metadata = getStrandMetadata(KAFKA_RESOURCE_ON_ERROR);
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
        MethodType consumerRecordMethodType = getOnConsumerRecordMethod(service).get();
        Parameter[] parameters = consumerRecordMethodType.getParameters();
        boolean callerExists = false;
        boolean consumerRecordsExists = false;
        boolean payloadExists = false;
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
                    if (isConsumerRecordsType(parameter, consumerRecordMethodType.getAnnotations())) {
                        if (consumerRecordsExists) {
                            throw KafkaUtils.createKafkaError("Invalid remote function signature");
                        }
                        consumerRecordsExists = true;
                        arguments[index++] = getConsumerRecords(records, (RecordType) getIntendedType(parameter.type),
                                parameter.type.isReadOnly());
                        arguments[index++] = true;
                    } else {
                        if (payloadExists) {
                            throw KafkaUtils.createKafkaError("Invalid remote function signature");
                        }
                        payloadExists = true;
                        arguments[index++] = getValuesWithIntendedType(parameter.type, records);
                        arguments[index++] = true;
                    }
                    break;
                default:
                    throw KafkaUtils.createKafkaError("Invalid remote function signature");
            }
        }
        return arguments;
    }

    private boolean isConsumerRecordsType(Parameter parameter, BMap<BString, Object> annotations) {
        if (annotations.containsKey(StringUtils.fromString(PARAM_ANNOTATION_PREFIX + parameter.name))) {
            BMap paramAnnotationMap = annotations.getMapValue(StringUtils.fromString(
                    PARAM_ANNOTATION_PREFIX + parameter.name));
            if (paramAnnotationMap.containsKey(PARAM_PAYLOAD_ANNOTATION_NAME)) {
                return false;
            }
        }
        return invokeIsAnydataConsumerRecordTypeMethod(getIntendedType(parameter.type));
    }

    private BObject createCaller(BObject listener) {
        BObject caller = ValueCreator.createObjectValue(ModuleUtils.getModule(), KafkaConstants.CALLER_STRUCT_NAME);
        KafkaConsumer consumer = (KafkaConsumer) listener.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) listener.getNativeData(NATIVE_CONSUMER_CONFIG);
        caller.addNativeData(NATIVE_CONSUMER, consumer);
        caller.addNativeData(NATIVE_CONSUMER_CONFIG, consumerProperties);
        return caller;
    }

    private Type getIntendedType(Type type) {
        if (type.getTag() == INTERSECTION_TAG) {
            return ((ArrayType) ((IntersectionType) type).getConstituentTypes().get(0)).getElementType();
        }
        return ((ArrayType) type).getElementType();
    }

    private Optional<MethodType> getOnConsumerRecordMethod(BObject service) {
        MethodType[] methodTypes = service.getType().getMethods();
        return Stream.of(methodTypes)
                .filter(methodType -> KAFKA_RESOURCE_ON_RECORD.equals(methodType.getName())).findFirst();
    }

    private boolean invokeIsAnydataConsumerRecordTypeMethod(Type paramType) {
        BObject client = ValueCreator.createObjectValue(ModuleUtils.getModule(), TYPE_CHECKER_OBJECT_NAME);
        Semaphore sem = new Semaphore(0);
        KafkaRecordTypeCheckCallback recordTypeCheckCallback = new KafkaRecordTypeCheckCallback(sem);
        StrandMetadata metadata = getStrandMetadata(KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD);
        bRuntime.invokeMethodAsyncSequentially(client, KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD, null, metadata,
                recordTypeCheckCallback, null, PredefinedTypes.TYPE_BOOLEAN,
                ValueCreator.createTypedescValue(paramType), true);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            throw createKafkaError(e.getMessage());
        }
        return recordTypeCheckCallback.getIsConsumerRecordType();
    }

    private BArray getValuesWithIntendedType(Type type, ConsumerRecords records) {
        Type intendedType = getIntendedType(type);
        BArray bArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(intendedType));
        for (Object record: records) {
            bArray.append(getValueWithIntendedType(intendedType, (byte[]) ((ConsumerRecord) record).value()));
        }
        if (type.isReadOnly()) {
            bArray.freezeDirect();
        }
        return bArray;
    }

    private StrandMetadata getStrandMetadata(String parentFunctionName) {
        return new StrandMetadata(ModuleUtils.getModule().getOrg(),
                    ModuleUtils.getModule().getName(), ModuleUtils.getModule().getMajorVersion(), parentFunctionName);
    }
}
