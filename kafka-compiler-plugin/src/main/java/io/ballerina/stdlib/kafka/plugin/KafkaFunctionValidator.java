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

import io.ballerina.compiler.syntax.tree.ArrayTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.IdentifierToken;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.QualifiedNameReferenceNode;
import io.ballerina.compiler.syntax.tree.RequiredParameterNode;
import io.ballerina.compiler.syntax.tree.ReturnTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.Token;
import io.ballerina.compiler.syntax.tree.TypeDescriptorNode;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.Objects;
import java.util.Optional;

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
            context.reportDiagnostic(PluginUtils.getDiagnostic(CompilationErrors.NO_ON_CONSUMER_RECORD,
                    DiagnosticSeverity.ERROR, serviceDeclarationNode.location()));
        }
    }

    private void validateOnConsumerRecord() {
        if (!PluginUtils.isRemoteFunction(context, onConsumerRecord)) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(
                    PluginConstants.CompilationErrors.FUNCTION_SHOULD_BE_REMOTE,
                    DiagnosticSeverity.ERROR, onConsumerRecord.functionSignature().location()));
        }
        SeparatedNodeList<ParameterNode> parameters = onConsumerRecord.functionSignature().parameters();
        validateFunctionParameters(parameters, onConsumerRecord);
        validateReturnTypeErrorOrNil(onConsumerRecord);
    }

    private void validateFunctionParameters(SeparatedNodeList<ParameterNode> parameters,
                                            FunctionDefinitionNode functionDefinitionNode) {
        if (parameters.size() > 1) {
            validateFirstParam(functionDefinitionNode, parameters.get(0));
            validateSecondParam(functionDefinitionNode, parameters.get(1));
        }
        if (parameters.size() > 2) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(CompilationErrors.ONLY_PARAMS_ALLOWED,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.functionSignature().location()));
        }
        if (parameters.size() < 2) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(CompilationErrors.MUST_HAVE_CALLER_AND_RECORDS,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.functionSignature().location()));
        }
    }

    private void validateFirstParam(FunctionDefinitionNode functionDefinitionNode, ParameterNode parameterNode) {
        RequiredParameterNode requiredParameterNode = (RequiredParameterNode) parameterNode;
        Node parameterTypeNode = requiredParameterNode.typeName();
        if (!parameterTypeNode.kind().equals(SyntaxKind.QUALIFIED_NAME_REFERENCE)) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(
                    CompilationErrors.INVALID_FUNCTION_PARAM_CALLER,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
        } else {
            Token modulePrefix = ((QualifiedNameReferenceNode) parameterTypeNode).modulePrefix();
            IdentifierToken identifierToken = ((QualifiedNameReferenceNode) parameterTypeNode).identifier();
            if (!modulePrefix.text().equals(PluginConstants.PACKAGE_PREFIX) ||
                    !identifierToken.text().equals(PluginConstants.CALLER)) {
                context.reportDiagnostic(PluginUtils.getDiagnostic(
                        CompilationErrors.INVALID_FUNCTION_PARAM_CALLER,
                        DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
            }
        }
    }

    private void validateSecondParam(FunctionDefinitionNode functionDefinitionNode,
                                     ParameterNode parameterNode) {
        RequiredParameterNode requiredParameterNode = (RequiredParameterNode) parameterNode;
        Node parameterTypeNode = requiredParameterNode.typeName();
        if (!parameterTypeNode.kind().equals(SyntaxKind.ARRAY_TYPE_DESC)) {
            context.reportDiagnostic(PluginUtils.getDiagnostic(
                    CompilationErrors.INVALID_FUNCTION_PARAM_RECORDS,
                    DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
        } else {
            ArrayTypeDescriptorNode arrayTypeDescriptorNode = (ArrayTypeDescriptorNode) parameterTypeNode;
            TypeDescriptorNode memberType = arrayTypeDescriptorNode.memberTypeDesc();
            if (memberType.kind() != SyntaxKind.QUALIFIED_NAME_REFERENCE) {
                context.reportDiagnostic(PluginUtils.getDiagnostic(
                        CompilationErrors.INVALID_FUNCTION_PARAM_RECORDS,
                        DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
            } else {
                Token modulePrefix =
                        ((QualifiedNameReferenceNode) arrayTypeDescriptorNode.memberTypeDesc()).modulePrefix();
                IdentifierToken identifierToken =
                        ((QualifiedNameReferenceNode) arrayTypeDescriptorNode.memberTypeDesc()).identifier();
                if (!modulePrefix.text().equals(PluginConstants.PACKAGE_PREFIX) ||
                        !identifierToken.text().equals(PluginConstants.RECORD_PARAM)) {
                    context.reportDiagnostic(PluginUtils.getDiagnostic(
                            CompilationErrors.INVALID_FUNCTION_PARAM_RECORDS,
                            DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
                }
            }
        }
    }

    private void validateReturnTypeErrorOrNil(FunctionDefinitionNode functionDefinitionNode) {
        Optional<ReturnTypeDescriptorNode> returnTypes = functionDefinitionNode.functionSignature().returnTypeDesc();
        if (returnTypes.isPresent()) {
            ReturnTypeDescriptorNode returnTypeDescriptorNode = returnTypes.get();
            Node returnNodeType = returnTypeDescriptorNode.type();
            String returnType = returnNodeType.toString().split(" ")[0];
            if (!returnType.equals(PluginConstants.ERROR_OR_NIL) &&
                    !returnType.equals(PluginConstants.NIL_OR_ERROR) &&
                    !returnType.equals(PluginConstants.KAFKA_ERROR_OR_NIL) &&
                    !returnType.equals(PluginConstants.NIL_OR_KAFKA_ERROR)) {
                context.reportDiagnostic(PluginUtils.getDiagnostic(
                        PluginConstants.CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL,
                        DiagnosticSeverity.ERROR, functionDefinitionNode.location()));
            }
        }
    }
}
