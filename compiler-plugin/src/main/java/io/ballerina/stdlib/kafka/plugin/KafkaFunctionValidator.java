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

import io.ballerina.compiler.api.SemanticModel;
import io.ballerina.compiler.api.symbols.MethodSymbol;
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.syntax.tree.ArrayTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.IntersectionTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.RequiredParameterNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.TypeDescriptorNode;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.Location;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.ballerina.compiler.syntax.tree.SyntaxKind.ANYDATA_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.ARRAY_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.BOOLEAN_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.BYTE_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.DECIMAL_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.ERROR_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.FLOAT_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.INT_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.JSON_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.MAP_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.QUALIFIED_NAME_REFERENCE;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.RECORD_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.SIMPLE_NAME_REFERENCE;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.STRING_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.TABLE_TYPE_DESC;
import static io.ballerina.compiler.syntax.tree.SyntaxKind.XML_TYPE_DESC;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CALLER;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.FUNCTION_SHOULD_BE_REMOTE;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_PARAM_COUNT;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_PARAM_TYPES;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_SINGLE_PARAMETER;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.MUST_HAVE_CALLER_AND_RECORDS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.MUST_HAVE_ERROR;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.NO_ON_CONSUMER_RECORD;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.ONLY_ERROR_ALLOWED;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.ERROR_PARAM;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.RECORD_PARAM;
import static io.ballerina.stdlib.kafka.plugin.PluginUtils.getDiagnostic;
import static io.ballerina.stdlib.kafka.plugin.PluginUtils.getMethodSymbol;
import static io.ballerina.stdlib.kafka.plugin.PluginUtils.validateModuleId;
import static io.ballerina.tools.diagnostics.DiagnosticSeverity.ERROR;

/**
 * Kafka remote function validator.
 */
public class KafkaFunctionValidator {

    private final SyntaxNodeAnalysisContext context;
    private final ServiceDeclarationNode serviceDeclarationNode;
    private final SemanticModel semanticModel;
    FunctionDefinitionNode onConsumerRecord;
    FunctionDefinitionNode onError;

    public KafkaFunctionValidator(SyntaxNodeAnalysisContext context, FunctionDefinitionNode onConsumerRecord,
                                  FunctionDefinitionNode onError) {
        this.context = context;
        this.serviceDeclarationNode = (ServiceDeclarationNode) context.node();
        this.onConsumerRecord = onConsumerRecord;
        this.onError = onError;
        this.semanticModel = context.semanticModel();
    }

    public void validate() {
        validateMandatoryFunction();
        if (Objects.nonNull(onConsumerRecord)) {
            validateOnConsumerRecord();
        }
        if (Objects.nonNull(onError)) {
            validateOnError();
        }
    }

    private void validateMandatoryFunction() {
        if (Objects.isNull(onConsumerRecord)) {
            reportErrorDiagnostic(NO_ON_CONSUMER_RECORD, serviceDeclarationNode.location());
        }
    }

    private void validateOnConsumerRecord() {
        if (!PluginUtils.isRemoteFunction(context, onConsumerRecord)) {
            reportErrorDiagnostic(FUNCTION_SHOULD_BE_REMOTE, onConsumerRecord.functionSignature().location());
        }
        validateOnConsumerRecordParameters(onConsumerRecord);
        validateReturnTypeErrorOrNil(onConsumerRecord);
    }

    private void validateOnError() {
        if (!PluginUtils.isRemoteFunction(context, onError)) {
            reportErrorDiagnostic(FUNCTION_SHOULD_BE_REMOTE, onError.functionSignature().location());
        }
        validateOnErrorParameters(onError);
        validateReturnTypeErrorOrNil(onError);
    }

    private void validateOnErrorParameters(FunctionDefinitionNode functionDefinitionNode) {
        SeparatedNodeList<ParameterNode> parameters = functionDefinitionNode.functionSignature().parameters();
        if (parameters.size() == 1) {
            ParameterNode paramNode = parameters.get(0);
            SyntaxKind paramSyntaxKind = ((RequiredParameterNode) paramNode).typeName().kind();
            if (paramSyntaxKind.equals(QUALIFIED_NAME_REFERENCE)) {
                Node parameterTypeNode = ((RequiredParameterNode) paramNode).typeName();
                Optional<Symbol> paramSymbol = semanticModel.symbol(parameterTypeNode);
                if (!paramSymbol.get().getName().get().equals(ERROR_PARAM) ||
                        !validateModuleId(paramSymbol.get().getModule().get())) {
                    reportErrorDiagnostic(ONLY_ERROR_ALLOWED, paramNode.location());
                }
            } else if (!paramSyntaxKind.equals(ERROR_TYPE_DESC)) {
                reportErrorDiagnostic(ONLY_ERROR_ALLOWED, paramNode.location());
            }
        } else if (parameters.size() > 1) {
            reportErrorDiagnostic(ONLY_ERROR_ALLOWED, functionDefinitionNode.functionSignature().location());
        } else {
            reportErrorDiagnostic(MUST_HAVE_ERROR, functionDefinitionNode.functionSignature().location());
        }
    }

    private void validateOnConsumerRecordParameters(FunctionDefinitionNode functionDefinitionNode) {
        SeparatedNodeList<ParameterNode> parameters = functionDefinitionNode.functionSignature().parameters();
        Location location = functionDefinitionNode.functionSignature().location();
        if (parameters.size() > 3) {
            reportErrorDiagnostic(INVALID_PARAM_COUNT, location);
            return;
        } else if (parameters.size() < 1) {
             reportErrorDiagnostic(MUST_HAVE_CALLER_AND_RECORDS, location);
             return;
        }
        validateParameterTypes(parameters, location);
    }

    private void validateParameterTypes(SeparatedNodeList<ParameterNode> parameters, Location location) {
        boolean callerExists = false;
        boolean consumerRecordsExists = false;
        boolean dataExists = false;
        for (ParameterNode paramNode: parameters) {
            RequiredParameterNode requiredParameterNode = (RequiredParameterNode) paramNode;
            SyntaxKind paramSyntaxKind = requiredParameterNode.typeName().kind();
            switch (paramSyntaxKind) {
                case ARRAY_TYPE_DESC:
                    if (!consumerRecordsExists) {
                        consumerRecordsExists = validateConsumerRecordsParam(requiredParameterNode);
                    }
                    if (!dataExists) {
                        dataExists = validateDataParam(requiredParameterNode);
                    }
                    break;
                case INTERSECTION_TYPE_DESC:
                    if (!consumerRecordsExists) {
                        consumerRecordsExists = validateReadonlyConsumerRecordsParam(requiredParameterNode);
                    }
                    if (!dataExists) {
                        dataExists = validateReadonlyDataParam(requiredParameterNode);
                    }
                    break;
                case QUALIFIED_NAME_REFERENCE:
                    callerExists = validateCallerParam(requiredParameterNode);
                    break;
                default:
                    break;
            }
        }
        validateParameterTypeResults(parameters, location, callerExists, consumerRecordsExists, dataExists);
    }

    private void validateParameterTypeResults(SeparatedNodeList<ParameterNode> parameters, Location location,
                                              boolean callerExists, boolean consumerRecordsExists, boolean dataExists) {
        if (parameters.size() == 3) {
            if (!callerExists || !consumerRecordsExists || !dataExists) {
                reportErrorDiagnostic(INVALID_PARAM_TYPES, location);
            }
        } else if (parameters.size() == 2) {
            if ((!callerExists || !consumerRecordsExists) && (!callerExists || !dataExists)
                    && (!dataExists || !consumerRecordsExists)) {
                reportErrorDiagnostic(INVALID_PARAM_TYPES, location);
            }
        } else if (!consumerRecordsExists && !dataExists) {
            reportErrorDiagnostic(INVALID_SINGLE_PARAMETER, location);
        }
    }

    private boolean validateCallerParam(RequiredParameterNode requiredParameterNode) {
        Node parameterTypeNode = requiredParameterNode.typeName();
        Optional<Symbol> paramSymbol = semanticModel.symbol(parameterTypeNode);
        if (paramSymbol.isPresent()) {
            Optional<ModuleSymbol> moduleSymbol = paramSymbol.get().getModule();
            if (moduleSymbol.isPresent()) {
                String paramName = paramSymbol.get().getName().isPresent() ? paramSymbol.get().getName().get() : "";
                if (validateModuleId(moduleSymbol.get()) && paramName.equals(CALLER)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean validateConsumerRecordsParam(RequiredParameterNode requiredParameterNode) {
        Node parameterTypeNode = requiredParameterNode.typeName();
        ArrayTypeDescriptorNode arrayTypeDescriptorNode = (ArrayTypeDescriptorNode) parameterTypeNode;
        TypeDescriptorNode memberType = arrayTypeDescriptorNode.memberTypeDesc();
        Optional<Symbol> paramSymbol = semanticModel.symbol(memberType);
        if (paramSymbol.isPresent()) {
            Optional<ModuleSymbol> moduleSymbol = paramSymbol.get().getModule();
            if (moduleSymbol.isPresent()) {
                String paramName = paramSymbol.get().getName().isPresent() ? paramSymbol.get().getName().get() : "";
                if (validateModuleId(moduleSymbol.get()) && paramName.equals(PluginConstants.RECORD_PARAM)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean validateReadonlyConsumerRecordsParam(RequiredParameterNode requiredParameterNode) {
        Node parameterTypeNode = requiredParameterNode.typeName();
        IntersectionTypeDescriptorNode typeDescriptorNode = (IntersectionTypeDescriptorNode) parameterTypeNode;
        Optional<TypeDescriptorNode> arrayTypeDescNode = Optional.empty();
        if (typeDescriptorNode.rightTypeDesc().kind() == ARRAY_TYPE_DESC) {
            arrayTypeDescNode = Optional.of(((ArrayTypeDescriptorNode) typeDescriptorNode.rightTypeDesc())
                    .memberTypeDesc());
        } else if (typeDescriptorNode.leftTypeDesc().kind() == ARRAY_TYPE_DESC) {
            arrayTypeDescNode = Optional.of(((ArrayTypeDescriptorNode) typeDescriptorNode.leftTypeDesc())
                    .memberTypeDesc());
        }
        if (arrayTypeDescNode.isEmpty()) {
            return false;
        }
        Optional<Symbol> typeSymbol = semanticModel.symbol(arrayTypeDescNode.get());
        return typeSymbol.isPresent() && typeSymbol.get().nameEquals(RECORD_PARAM) &&
                typeSymbol.get().getModule().isPresent() && validateModuleId(typeSymbol.get().getModule().get());
    }

    private boolean validateDataParam(RequiredParameterNode requiredParameterNode) {
        Node parameterTypeNode = requiredParameterNode.typeName();
        ArrayTypeDescriptorNode arrayTypeDescriptorNode = (ArrayTypeDescriptorNode) parameterTypeNode;
        TypeDescriptorNode memberType = arrayTypeDescriptorNode.memberTypeDesc();
        SyntaxKind syntaxKind = memberType.kind();
        if (syntaxKind == QUALIFIED_NAME_REFERENCE) {
            return !validateConsumerRecordsParam(requiredParameterNode);
        } else {
            return validateDataParamSyntaxKind(syntaxKind);
        }
    }

    private boolean validateReadonlyDataParam(RequiredParameterNode requiredParameterNode) {
        Node parameterTypeNode = requiredParameterNode.typeName();
        IntersectionTypeDescriptorNode typeDescriptorNode = (IntersectionTypeDescriptorNode) parameterTypeNode;
        if (typeDescriptorNode.rightTypeDesc().kind() == ARRAY_TYPE_DESC) {
            SyntaxKind syntaxKind = ((ArrayTypeDescriptorNode) typeDescriptorNode.rightTypeDesc())
                    .memberTypeDesc().kind();
            return validateDataParamSyntaxKind(syntaxKind);
        } else if (typeDescriptorNode.leftTypeDesc().kind() == ARRAY_TYPE_DESC) {
            SyntaxKind syntaxKind = ((ArrayTypeDescriptorNode) typeDescriptorNode.leftTypeDesc())
                    .memberTypeDesc().kind();
            return validateDataParamSyntaxKind(syntaxKind);
        }
        return false;
    }

    private boolean validateDataParamSyntaxKind(SyntaxKind syntaxKind) {
        return syntaxKind == INT_TYPE_DESC || syntaxKind == STRING_TYPE_DESC || syntaxKind == BOOLEAN_TYPE_DESC ||
                syntaxKind == FLOAT_TYPE_DESC || syntaxKind == DECIMAL_TYPE_DESC || syntaxKind == RECORD_TYPE_DESC ||
                syntaxKind == SIMPLE_NAME_REFERENCE || syntaxKind == MAP_TYPE_DESC || syntaxKind == BYTE_TYPE_DESC ||
                syntaxKind == TABLE_TYPE_DESC || syntaxKind == JSON_TYPE_DESC || syntaxKind == XML_TYPE_DESC ||
                syntaxKind == ANYDATA_TYPE_DESC;
    }

    private void validateReturnTypeErrorOrNil(FunctionDefinitionNode functionDefinitionNode) {
        MethodSymbol methodSymbol = getMethodSymbol(context, functionDefinitionNode);
        if (methodSymbol != null) {
            Optional<TypeSymbol> returnTypeDesc = methodSymbol.typeDescriptor().returnTypeDescriptor();
            if (returnTypeDesc.isPresent()) {
                if (returnTypeDesc.get().typeKind() == TypeDescKind.UNION) {
                    List<TypeSymbol> returnTypeMembers =
                            ((UnionTypeSymbol) returnTypeDesc.get()).memberTypeDescriptors();
                    for (TypeSymbol returnType : returnTypeMembers) {
                        if (returnType.typeKind() != TypeDescKind.NIL) {
                            if (returnType.typeKind() == TypeDescKind.TYPE_REFERENCE) {
                                if (!returnType.signature().equals(PluginConstants.ERROR) &&
                                        !validateModuleId(returnType.getModule().get())) {
                                    reportErrorDiagnostic(INVALID_RETURN_TYPE_ERROR_OR_NIL,
                                            functionDefinitionNode.location());
                                }
                            } else if (returnType.typeKind() != TypeDescKind.ERROR) {
                                reportErrorDiagnostic(INVALID_RETURN_TYPE_ERROR_OR_NIL,
                                        functionDefinitionNode.location());
                            }
                        }
                    }
                } else if (returnTypeDesc.get().typeKind() != TypeDescKind.NIL) {
                    reportErrorDiagnostic(INVALID_RETURN_TYPE_ERROR_OR_NIL, functionDefinitionNode.location());
                }
            }
        }
    }

    public void reportErrorDiagnostic(PluginConstants.CompilationErrors error, Location location) {
        context.reportDiagnostic(getDiagnostic(error, ERROR, location));
    }
}
