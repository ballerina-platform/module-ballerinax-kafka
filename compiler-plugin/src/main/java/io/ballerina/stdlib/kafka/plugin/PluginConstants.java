/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.kafka.plugin;

/**
 * Kafka compiler plugin constants.
 */
public class PluginConstants {
    // compiler plugin constants
    public static final String PACKAGE_PREFIX = "kafka";
    public static final String REMOTE_QUALIFIER = "REMOTE";
    public static final String ON_RECORDS_FUNC = "onConsumerRecord";
    public static final String PACKAGE_ORG = "ballerinax";

    // parameters
    public static final String CALLER = "Caller";
    public static final String RECORD_PARAM = "ConsumerRecord";
    public static final String ERROR_PARAM = "Error";

    // return types error or nil
    public static final String ERROR = "error";

    /**
     * Compilation errors.
     */
    enum CompilationErrors {
        NO_ON_CONSUMER_RECORD("Service must have remote method onConsumerRecord.",
                "KAFKA_101"),
        INVALID_REMOTE_FUNCTION("Invalid remote method.", "KAFKA_102"),
        INVALID_FUNCTION("Invalid remote method.", "KAFKA_103"),
        FUNCTION_SHOULD_BE_REMOTE("Method must have the remote qualifier.", "KAFKA_104"),
        MUST_HAVE_CALLER_AND_RECORDS("Must have the method parameters kafka:Caller and kafka:ConsumerRecord[].",
                "KAFKA_105"),
        INVALID_FUNCTION_PARAM_CALLER("Invalid method parameter. Only kafka:Caller is allowed.",
                "KAFKA_106"),
        INVALID_FUNCTION_PARAM_RECORDS("Invalid method parameter. Only kafka:ConsumerRecord[] is allowed.",
                "KAFKA_107"),
        ONLY_PARAMS_ALLOWED("Invalid method parameter count. " +
                "Only kafka:Caller and kafka:ConsumerRecord[] are allowed.", "KAFKA_108"),
        INVALID_RETURN_TYPE_ERROR_OR_NIL("Invalid return type. Only error? or kafka:Error? is allowed.",
                "KAFKA_109"),
        INVALID_MULTIPLE_LISTENERS("Multiple listener attachments. Only one kafka:Listener is allowed.",
                "KAFKA_110"),
        INVALID_ANNOTATION_NUMBER("No annotations are allowed for kafka services.", "KAFKA_111");
        TEMPLATE_CODE_GENERATION_HINT("Template generation for empty service", "KAFKA_112");

        private final String error;
        private final String errorCode;

        CompilationErrors(String error, String errorCode) {
            this.error = error;
            this.errorCode = errorCode;
        }

        String getError() {
            return error;
        }

        String getErrorCode() {
            return errorCode;
        }
    }

    private PluginConstants() {
    }
}
