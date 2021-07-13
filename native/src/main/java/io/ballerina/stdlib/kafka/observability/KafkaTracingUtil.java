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

package io.ballerina.stdlib.kafka.observability;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.ObserverContext;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;

/**
 * Providing tracing functionality to Kafka.
 *
 * @since 1.2.0
 */
public class KafkaTracingUtil {

    public static void traceResourceInvocation(Environment environment, BObject object, String topic) {
        if (!ObserveUtils.isTracingEnabled()) {
            return;
        }
        ObserverContext observerContext = ObserveUtils.getObserverContextOfCurrentFrame(environment);
        if (observerContext == null) {
            observerContext = new ObserverContext();
            ObserveUtils.setObserverContextToCurrentFrame(environment, observerContext);
        }
        setTags(observerContext, object, topic);
    }

    public static void traceResourceInvocation(Environment environment, BObject object) {
        if (!ObserveUtils.isTracingEnabled()) {
            return;
        }
        ObserverContext observerContext = ObserveUtils.getObserverContextOfCurrentFrame(environment);
        if (observerContext == null) {
            observerContext = new ObserverContext();
            ObserveUtils.setObserverContextToCurrentFrame(environment, observerContext);
        }
        setTags(observerContext, object);
    }

    private static void setTags(ObserverContext observerContext, BObject object, String topic) {
        observerContext.addTag(KafkaObservabilityConstants.TAG_URL, KafkaUtils.getBootstrapServers(object));
        observerContext.addTag(KafkaObservabilityConstants.TAG_CLIENT_ID, KafkaUtils.getClientId(object));
        observerContext.addTag(KafkaObservabilityConstants.TAG_TOPIC, topic);
    }

    private static void setTags(ObserverContext observerContext, BObject object) {
        observerContext.addTag(KafkaObservabilityConstants.TAG_URL, KafkaUtils.getBootstrapServers(object));
        observerContext.addTag(KafkaObservabilityConstants.TAG_CLIENT_ID, KafkaUtils.getClientId(object));
    }

    private KafkaTracingUtil() {
    }
}
