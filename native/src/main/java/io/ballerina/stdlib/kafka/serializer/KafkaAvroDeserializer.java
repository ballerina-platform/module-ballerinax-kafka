/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

package io.ballerina.stdlib.kafka.serializer;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;

public class KafkaAvroDeserializer implements Deserializer<Object> {
    BObject deserializer = null;
    public static final String DESERIALIZER_CLASS = "KafkaAvroDeserializer";
    public static final String DESERIALIZE_FUNCTION = "deserialize";
    public static final String DESERIALIZER_NOT_FOUND_ERROR = "Deserializer is not found";

    public KafkaAvroDeserializer(BString schemaRegistryUrl, BMap<BString, Object> originals,
                                 BMap<BString, Object> headers) throws BError {
        try {
            Object[] arguments = new Object[]{schemaRegistryUrl, originals, headers};
            this.deserializer = (schemaRegistryUrl != null) ? ValueCreator.createObjectValue(ModuleUtils.getModule(),
                    DESERIALIZER_CLASS, arguments) : null;
        } catch (BError e) {
            this.deserializer = null;
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (this.deserializer == null) {
            return createKafkaError(DESERIALIZER_NOT_FOUND_ERROR);
        }
        BArray value = ValueCreator.createArrayValue(data);
        Object[] arguments = new Object[]{value};
        return ModuleUtils.getEnvironment()
                .getRuntime().callMethod(this.deserializer, DESERIALIZE_FUNCTION, null, arguments);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }
}
