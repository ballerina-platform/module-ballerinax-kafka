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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.MethodType;
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static io.ballerina.runtime.api.types.TypeTags.ARRAY_TAG;
import static io.ballerina.runtime.api.types.TypeTags.INTERSECTION_TAG;
import static io.ballerina.runtime.api.types.TypeTags.OBJECT_TYPE_TAG;
import static io.ballerina.runtime.api.utils.TypeUtils.getReferredType;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSTRAINT_VALIDATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_CONFIG_FIELD_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_ERROR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_RECORD;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PARAM_ANNOTATION_PREFIX;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PARAM_PAYLOAD_ANNOTATION_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.TYPE_CHECKER_OBJECT_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAutoCommitConfig;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAutoSeekOnErrorConfig;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getConsumerRecords;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getValuesWithIntendedType;

/**
 * Kafka Connector Consumer for Ballerina.
 */
public class KafkaListenerImpl implements KafkaListener {

    private final BObject service;
    private final BObject listener;
    private final Environment env;

    public KafkaListenerImpl(BObject listener, BObject service, Environment env) {
        this.env = env;
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
    public void onError(Throwable t) {
        KafkaMetricsUtil.reportConsumerError(listener, KafkaObservabilityConstants.ERROR_TYPE_MSG_RECEIVED);
        Optional<MethodType> onErrorMethod = getOnErrorMethod(service);
        if (onErrorMethod.isPresent()) {
            executeOnError(onErrorMethod.get(), t);
        } else {
            t.printStackTrace();
        }
    }

    private void executeResource(BObject listener, KafkaPollCycleFutureListener consumer, ConsumerRecords records) {
        KafkaConsumer kafkaConsumer = (KafkaConsumer) listener.getNativeData(NATIVE_CONSUMER);
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        Thread.startVirtualThread(() -> {
            Map<String, Object> properties = null;
            if (ObserveUtils.isTracingEnabled()) {
                properties = getNewObserverContextInProperties(listener);
            }
            try {
                boolean isIsolated = serviceType.isIsolated() && serviceType.isIsolated(KAFKA_RESOURCE_ON_RECORD);
                StrandMetadata metadata = new StrandMetadata(isIsolated, properties);
                Object result = env.getRuntime().callMethod(service, KAFKA_RESOURCE_ON_RECORD, metadata,
                            getResourceParameters(service, this.listener, records, kafkaConsumer));
                consumer.notifySuccess(result);
            } catch (BError bError) {
                consumer.notifyFailure(bError);
                onError(bError);
            }
        });
    }

    private void executeOnError(MethodType onErrorMethod, Throwable throwable) {
        Object[] arguments = new Object[onErrorMethod.getParameters().length];
        if (throwable instanceof BError) {
            arguments[0] = throwable;
        } else {
            arguments[0] = createKafkaError(throwable.getMessage());
        }
        if (arguments.length == 2) {
            arguments[1] = createCaller(this.listener);
        }
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        Thread.startVirtualThread(() -> {
            Map<String, Object> properties = null;
            if (ObserveUtils.isTracingEnabled()) {
                properties = getNewObserverContextInProperties(listener);
            }
            try {
                boolean isIsolated = serviceType.isIsolated() && serviceType.isIsolated(KAFKA_RESOURCE_ON_ERROR);
                StrandMetadata metadata = new StrandMetadata(isIsolated, properties);
                Object result = env.getRuntime().callMethod(service, KAFKA_RESOURCE_ON_ERROR, metadata,
                            properties, arguments);
                (new KafkaOnErrorCallback()).notifySuccess(result);
            } catch (BError bError) {
                (new KafkaOnErrorCallback()).notifyFailure(bError);
            }
        });
    }

    private Map<String, Object> getNewObserverContextInProperties(BObject listener) {
        Map<String, Object> properties = new HashMap<>();
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(listener),
                                                                        KafkaUtils.getBootstrapServers(listener));
        properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
        return properties;
    }

    public Object[] getResourceParameters(BObject service, BObject listener, ConsumerRecords records,
                                          KafkaConsumer kafkaConsumer) {
        MethodType consumerRecordMethodType = getOnConsumerRecordMethod(service).get();
        Parameter[] parameters = consumerRecordMethodType.getParameters();
        boolean callerExists = false;
        boolean consumerRecordsExists = false;
        boolean payloadExists = false;
        Object[] arguments = new Object[parameters.length];
        int index = 0;
        for (Parameter parameter : parameters) {
            Type referredType = getReferredType(parameter.type);
            switch (referredType.getTag()) {
                case OBJECT_TYPE_TAG:
                    if (callerExists) {
                        throw createKafkaError("Invalid remote function signature");
                    }
                    callerExists = true;
                    arguments[index++] = createCaller(listener);
                    break;
                case INTERSECTION_TAG:
                case ARRAY_TAG:
                    boolean constraintValidation = (boolean) listener.getMapValue(CONSUMER_CONFIG_FIELD_NAME)
                            .get(CONSTRAINT_VALIDATION);
                    boolean autoCommit = getAutoCommitConfig(listener);
                    boolean autoSeek = getAutoSeekOnErrorConfig(listener);
                    if (isConsumerRecordsType(parameter, consumerRecordMethodType.getAnnotations())) {
                        if (consumerRecordsExists) {
                            throw createKafkaError("Invalid remote function signature");
                        }
                        consumerRecordsExists = true;
                        BArray consumerRecords = getConsumerRecords(records,
                                (RecordType) getIntendedType(referredType), referredType.isReadOnly(),
                                constraintValidation, autoCommit, kafkaConsumer, autoSeek);
                        arguments[index++] = consumerRecords;
                    } else {
                        if (payloadExists) {
                            throw createKafkaError("Invalid remote function signature");
                        }
                        payloadExists = true;
                        BArray payload = getValuesWithIntendedType(referredType, kafkaConsumer, records,
                                constraintValidation, autoCommit, autoSeek);
                        arguments[index++] = payload;
                    }
                    break;
                default:
                    throw createKafkaError("Invalid remote function signature");
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
        return invokeIsAnydataConsumerRecordTypeMethod(getIntendedType(getReferredType(parameter.type)));
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
            return getReferredType(((ArrayType) ((IntersectionType) type).getConstituentTypes().get(0))
                    .getElementType());
        }
        return getReferredType(getReferredType(((ArrayType) type).getElementType()));
    }

    private Optional<MethodType> getOnConsumerRecordMethod(BObject service) {
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        MethodType[] methodTypes = serviceType.getMethods();
        return Stream.of(methodTypes)
                .filter(methodType -> KAFKA_RESOURCE_ON_RECORD.equals(methodType.getName())).findFirst();
    }

    private Optional<MethodType> getOnErrorMethod(BObject service) {
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        MethodType[] methodTypes = serviceType.getMethods();
        return Stream.of(methodTypes)
                .filter(methodType -> KAFKA_RESOURCE_ON_ERROR.equals(methodType.getName())).findFirst();
    }

    private boolean invokeIsAnydataConsumerRecordTypeMethod(Type paramType) {
        BObject client = ValueCreator.createObjectValue(ModuleUtils.getModule(), TYPE_CHECKER_OBJECT_NAME);
        Semaphore sem = new Semaphore(0);
        KafkaRecordTypeCheckCallback recordTypeCheckCallback = new KafkaRecordTypeCheckCallback(sem);
        Thread.startVirtualThread(() -> {
            try {
                Object result = env.getRuntime().callMethod(client,
                        KAFKA_RESOURCE_IS_ANYDATA_CONSUMER_RECORD, new StrandMetadata(false, null),
                        ValueCreator.createTypedescValue(paramType));
                recordTypeCheckCallback.notifySuccess(result);
            } catch (BError bError) {
                recordTypeCheckCallback.notifyFailure(bError);
            }
        });
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            throw createKafkaError(e.getMessage());
        }
        return recordTypeCheckCallback.getIsConsumerRecordType();
    }

    static class KafkaOnErrorCallback {
        public void notifySuccess(Object result) {
            if (result instanceof BError) {
                ((BError) result).printStackTrace();
            }
        }

        public void notifyFailure(BError bError) {
            bError.printStackTrace();
            System.exit(1);
        }
    }
}
