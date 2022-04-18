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

package io.ballerina.stdlib.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getConsumerRecords;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getMilliSeconds;

/**
 * Native function polls the broker to retrieve messages within given timeout.
 */
public class Poll {

    // static init
    private static final ExecutorService executorService = Executors.newCachedThreadPool(new KafkaThreadFactory());

    public static Object poll(Environment env, BObject consumerObject, BDecimal timeout, BTypedesc bTypedesc) {
        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
        Future balFuture = env.markAsync();
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        RecordType recordType = getRecordType(bTypedesc);
        executorService.execute(()-> {
            try {
                Duration duration = Duration.ofMillis(getMilliSeconds(timeout));
                ConsumerRecords recordsRetrieved = kafkaConsumer.poll(duration);
                balFuture.complete(getConsumerRecords(recordsRetrieved, recordType,
                        bTypedesc.getDescribingType().isReadOnly()));
            } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                balFuture.complete(createKafkaError("Failed to poll from the Kafka server: " + e.getMessage()));
            }
        });
        return null;
    }

    public static Object pollPayload(Environment env, BObject consumerObject, BDecimal timeout, BTypedesc bTypedesc) {
        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
        Future balFuture = env.markAsync();
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        executorService.execute(()-> {
            try {
                Duration duration = Duration.ofMillis(getMilliSeconds(timeout));
                ConsumerRecords recordsRetrieved = kafkaConsumer.poll(duration);
                ArrayType arrayType = (ArrayType) bTypedesc.getDescribingType();
                BArray dataArray = ValueCreator.createArrayValue(arrayType);
                if (!recordsRetrieved.isEmpty()) {
                    dataArray = KafkaUtils.getValuesWithIntendedType(arrayType, recordsRetrieved);
                }
                balFuture.complete(dataArray);
            } catch (BError bError) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                balFuture.complete(createKafkaError(bError.getMessage()));
            } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                balFuture.complete(createKafkaError("Failed to poll from the Kafka server: " + e.getMessage()));
            }
        });
        return null;
    }

    private static RecordType getRecordType(BTypedesc bTypedesc) {
        RecordType recordType;
        if (bTypedesc.getDescribingType().isReadOnly()) {
            recordType = (RecordType) ((IntersectionType) ((ArrayType) bTypedesc.getDescribingType())
                    .getElementType()).getConstituentTypes().get(0);
        } else {
            recordType = (RecordType) ((ArrayType) bTypedesc.getDescribingType()).getElementType();
        }
        return recordType;
    }

    static class KafkaThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread ballerinaKafka = new Thread(r);
            ballerinaKafka.setName("balx-kafka-consumer-network-thread");
            return ballerinaKafka;
        }
    }
}
