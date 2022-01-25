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
import io.ballerina.compiler.api.symbols.ArrayTypeSymbol;
import io.ballerina.compiler.api.symbols.IntersectionTypeSymbol;
import io.ballerina.compiler.api.symbols.MethodSymbol;
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.ParameterSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.syntax.tree.ArrayTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.RequiredParameterNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.TypeDescriptorNode;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.FUNCTION_SHOULD_BE_REMOTE;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants
        .CompilationErrors.INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_FUNCTION_PARAM_RECORDS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.MUST_HAVE_CALLER_AND_RECORDS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.NO_ON_CONSUMER_RECORD;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.ONLY_PARAMS_ALLOWED;
import static io.ballerina.stdlib.kafka.plugin.PluginUtils.getMethodSymbol;
import static io.ballerina.stdlib.kafka.plugin.PluginUtils.validateModuleId;

/**
 * Kafka remote function validator.
 */
public class KafkaFunctionValidator {

    private final SyntaxNodeAnalysisContext context;
    private final ServiceDeclarationNode serviceDeclarationNode;
    FunctionDefinitionNode onConsumerRecord;

    public KafkaFunctionValidator(SyntaxNodeAnalysisContext context, FunctionDefinitionNode onConsumerRecord) {
        this.context = context;
        this.serviceDeclarationNode = (ServiceDeclarationNode) context.node();
        this.onConsumerRecord = onConsumerRecord;
    }

    public void validate() {
        validateMandatoryFunction();
        if (Objects.nonNull(onConsumerRecord)) {
            validateOnConsumerRecord();
        }
    }

    private void validateMandatoryFunction() {
        if (Objects.isNull(onConsumerRecord)) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(NO_ON_CONSUMER_RECORD,
                    DiagnosticSeverity.ERROR, serviceDeclarationNode.location()));
        }
    }

    private void validateOnConsumerRecord() {
        if (!PluginUtils.isRemoteFunction(context, onConsumerRecord)) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(FUNCTION_SHOULD_BE_REMOTE,
                    DiagnosticSeverity.ERROR, onConsumerRecord.functionSignature().location()));
        }
        SeparatedNodeList<ParameterNode> parameters = onConsumerRecord.functionSignature().parameters();
        validateFunctionParameters(parameters, onConsumerRecord);
        validateReturnTypeErrorOrNil(onConsumerRecord);
    }

    private void validateFunctionParameters(SeparatedNodeList<ParameterNode> parameters,
                                            FunctionDefinitionNode functionDefinitionNode) {
        // Here there can be caller and consumerRecords scenario + consumerRecords only scenario
        // If the param count is 1, checks are done for array_type and intersection_type
        // (kafka:ConsumerRecords[]/ kafka:ConsumerRecords[] & readonly)
        if (parameters.size() == 1) {
            ParameterNode paramNode = parameters.get(0);
            SyntaxKind paramSyntaxKind = ((RequiredParameterNode) paramNode).typeName().kind();
            if (paramSyntaxKind.equals(SyntaxKind.ARRAY_TYPE_DESC)) {
                validateConsumerRecordsParam(paramNode, MUST_HAVE_CALLER_AND_RECORDS);
            } else if (paramSyntaxKind.equals(SyntaxKind.INTERSECTION_TYPE_DESC)) {
                validateIntersectionParam(paramNode);
            } else {
                context.reportDiagnostic(PluginUtils.getDiagnostic(
                        CompilationErrors.MUST_HAVE_CALLER_AND_RECORDS,
                        DiagnosticSeverity.ERROR, paramNode.location()));
            }
        } else if (parameters.size() == 2) {
            ParameterNode firstParamNode = parameters.get(0);
            ParameterNode secondParamNode = parameters.get(1);
            SyntaxKind firstParamSyntaxKind = ((RequiredParameterNode) firstParamNode).typeName().kind();
            SyntaxKind secondParamSyntaxKind = ((RequiredParameterNode) secondParamNode).typeName().kind();
            // If the second parameter is a qualified_name_ref, try to validate it for caller and try to validate
            // first param for kafka:ConsumerRecords
            if (secondParamSyntaxKind.equals(SyntaxKind.QUALIFIED_NAME_REFERENCE)) {
                boolean callerResult = validateCallerParam(secondParamNode);
                if (firstParamSyntaxKind.equals(SyntaxKind.ARRAY_TYPE_DESC)) {
                    validateConsumerRecordsParam(firstParamNode, INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS);
                } else if (firstParamSyntaxKind.equals(SyntaxKind.INTERSECTION_TYPE_DESC)) {
                    validateIntersectionParam(firstParamNode);
                } else {
                    // If the second parameter is not a caller type and first param is not ConsumerRecords,
                    // the first parameter may be a caller type
                    // (eg: (kafka:Caller caller, kafka:Consumer records))
                    if (!callerResult) {
                        validateCallerParam(firstParamNode);
                    } else {
                        context.reportDiagnostic(PluginUtils.getDiagnostic(
                                CompilationErrors.INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS,
                                DiagnosticSeverity.ERROR, firstParamNode.location()));
                    }
                }
            // If the first parameter is a qualified_name_ref, try to validate it for caller and try to validate
            // second param for kafka:ConsumerRecords
            } else if (firstParamSyntaxKind.equals(SyntaxKind.QUALIFIED_NAME_REFERENCE)) {
                boolean callerResult = validateCallerParam(firstParamNode);
                if (secondParamSyntaxKind.equals(SyntaxKind.ARRAY_TYPE_DESC)) {
                    validateConsumerRecordsParam(secondParamNode, INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS);
                } else if (secondParamSyntaxKind.equals(SyntaxKind.INTERSECTION_TYPE_DESC)) {
                    validateIntersectionParam(secondParamNode);
                } else {
                    if (!callerResult) {
                        validateCallerParam(secondParamNode);
                    } else {
                        context.reportDiagnostic(PluginUtils.getDiagnostic(
                                CompilationErrors.INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS,
                                DiagnosticSeverity.ERROR, secondParamNode.location()));
                    }
                }
            } else {
                context.reportDiagnostic(PluginUtils.getDiagnostic(
                        CompilationErrors.INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS,
                        DiagnosticSeverity.ERROR, functionDefinitionNode.functionSignature().location()));
            }
        } else if (parameters.size() > 2) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(ONLY_PARAMS_ALLOWED,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.functionSignature().location()));
        } else {
            context.reportDiagnostic(PluginUtils.getDiagnostic(MUST_HAVE_CALLER_AND_RECORDS,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.functionSignature().location()));
        }
    }

    private boolean validateCallerParam(ParameterNode parameterNode) {
        RequiredParameterNode requiredParameterNode = (RequiredParameterNode) parameterNode;
        Node parameterTypeNode = requiredParameterNode.typeName();
        SemanticModel semanticModel = context.semanticModel();
        Optional<Symbol> paramSymbol = semanticModel.symbol(parameterTypeNode);
        if (paramSymbol.isPresent()) {
            Optional<ModuleSymbol> moduleSymbol = paramSymbol.get().getModule();
            if (moduleSymbol.isPresent()) {
                String paramName = paramSymbol.get().getName().isPresent() ?
                        paramSymbol.get().getName().get() : "";
                if (!validateModuleId(moduleSymbol.get()) ||
                        !paramName.equals(PluginConstants.CALLER)) {
                    context.reportDiagnostic(PluginUtils.getDiagnostic(
                            INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS,
                            DiagnosticSeverity.ERROR, requiredParameterNode.location()));
                    return false;
                }
                return true;
            }
        }
        context.reportDiagnostic(PluginUtils.getDiagnostic(
                INVALID_FUNCTION_PARAM_CALLER_OR_RECORDS,
                DiagnosticSeverity.ERROR, requiredParameterNode.location()));
        return false;
    }

    private void validateConsumerRecordsParam(ParameterNode parameterNode, CompilationErrors errorToThrow) {
        RequiredParameterNode requiredParameterNode = (RequiredParameterNode) parameterNode;
        Node parameterTypeNode = requiredParameterNode.typeName();
        ArrayTypeDescriptorNode arrayTypeDescriptorNode = (ArrayTypeDescriptorNode) parameterTypeNode;
        TypeDescriptorNode memberType = arrayTypeDescriptorNode.memberTypeDesc();
        SemanticModel semanticModel = context.semanticModel();
        Optional<Symbol> paramSymbol = semanticModel.symbol(memberType);
        if (paramSymbol.isPresent()) {
            Optional<ModuleSymbol> moduleSymbol = paramSymbol.get().getModule();
            if (moduleSymbol.isPresent()) {
                String paramName = paramSymbol.get().getName().isPresent() ?
                        paramSymbol.get().getName().get() : "";
                if (!validateModuleId(moduleSymbol.get()) ||
                        !paramName.equals(PluginConstants.RECORD_PARAM)) {
                    context.reportDiagnostic(PluginUtils.getDiagnostic(errorToThrow,
                            DiagnosticSeverity.ERROR, requiredParameterNode.location()));
                }
            } else {
                context.reportDiagnostic(PluginUtils.getDiagnostic(errorToThrow,
                        DiagnosticSeverity.ERROR, requiredParameterNode.location()));
            }
        }
    }

    private void validateIntersectionParam(ParameterNode parameterNode) {
        RequiredParameterNode requiredParameterNode = (RequiredParameterNode) parameterNode;
        SemanticModel semanticModel = context.semanticModel();
        Optional<Symbol> symbol = semanticModel.symbol(requiredParameterNode);
        if (symbol.isPresent()) {
            ParameterSymbol parameterSymbol = (ParameterSymbol) symbol.get();
            if (parameterSymbol.typeDescriptor() instanceof  IntersectionTypeSymbol) {
                IntersectionTypeSymbol intersectionTypeSymbol =
                        (IntersectionTypeSymbol) parameterSymbol.typeDescriptor();
                List<TypeSymbol> intersectionMembers = intersectionTypeSymbol.memberTypeDescriptors();
                ArrayTypeSymbol typeReferenceTypeSymbol = null;
                for (TypeSymbol typeSymbol : intersectionMembers) {
                    if (typeSymbol.typeKind() == TypeDescKind.ARRAY) {
                        typeReferenceTypeSymbol = (ArrayTypeSymbol) typeSymbol;
                    }
                }
                if (typeReferenceTypeSymbol != null) {
                    String paramName = typeReferenceTypeSymbol.memberTypeDescriptor().getName().isPresent() ?
                            typeReferenceTypeSymbol.memberTypeDescriptor().getName().get() : "";
                    if (!validateModuleId(typeReferenceTypeSymbol.memberTypeDescriptor().getModule().get()) ||
                            !paramName.equals(PluginConstants.RECORD_PARAM)) {
                        context.reportDiagnostic(PluginUtils.getDiagnostic(INVALID_FUNCTION_PARAM_RECORDS,
                                DiagnosticSeverity.ERROR, requiredParameterNode.location()));
                    }
                } else {
                    context.reportDiagnostic(PluginUtils.getDiagnostic(INVALID_FUNCTION_PARAM_RECORDS,
                            DiagnosticSeverity.ERROR, requiredParameterNode.location()));
                }
            } else {
                context.reportDiagnostic(PluginUtils.getDiagnostic(INVALID_FUNCTION_PARAM_RECORDS,
                        DiagnosticSeverity.ERROR, requiredParameterNode.location()));
            }
        } else {
            context.reportDiagnostic(PluginUtils.getDiagnostic(INVALID_FUNCTION_PARAM_RECORDS,
                    DiagnosticSeverity.ERROR, requiredParameterNode.location()));
        }
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
                                    context.reportDiagnostic(PluginUtils.getDiagnostic(
                                            CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL,
                                            DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
                                }
                            } else if (returnType.typeKind() != TypeDescKind.ERROR) {
                                context.reportDiagnostic(PluginUtils.getDiagnostic(
                                        CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL,
                                        DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
                            }
                        }
                    }
                } else if (returnTypeDesc.get().typeKind() != TypeDescKind.NIL) {
                    context.reportDiagnostic(PluginUtils.getDiagnostic(
                            CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL,
                            DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
                }
            }
        }
    }
}
