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

import io.ballerina.projects.DiagnosticResult;
import io.ballerina.projects.Package;
import io.ballerina.projects.PackageCompilation;
import io.ballerina.projects.directory.BuildProject;
import io.ballerina.tools.diagnostics.Diagnostic;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.ballerina.stdlib.kafka.plugin.CompilerPluginTestUtils.BALLERINA_SOURCES;
import static io.ballerina.stdlib.kafka.plugin.CompilerPluginTestUtils.RESOURCE_DIRECTORY;
import static io.ballerina.stdlib.kafka.plugin.CompilerPluginTestUtils.getEnvironmentBuilder;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.FUNCTION_SHOULD_BE_REMOTE;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_MULTIPLE_LISTENERS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_PARAM_COUNT;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_PARAM_TYPES;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_REMOTE_FUNCTION;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_RESOURCE_FUNCTION;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.INVALID_SINGLE_PARAMETER;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.MUST_HAVE_CALLER_AND_RECORDS;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.MUST_HAVE_ERROR;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.NO_ON_CONSUMER_RECORD;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.ONLY_CALLER_ALLOWED;
import static io.ballerina.stdlib.kafka.plugin.PluginConstants.CompilationErrors.ONLY_ERROR_ALLOWED;

/**
 * Tests for Kafka package compiler plugin.
 */
public class KafkaServiceValidationTest {

    @Test(enabled = true, description = "Service validating return types")
    public void testValidService1() {
        Package currentPackage = loadPackage("valid_service_1");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validating import as")
    public void testValidService2() {
        Package currentPackage = loadPackage("valid_service_2");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate basic service")
    public void testValidService3() {
        Package currentPackage = loadPackage("valid_service_3");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validating using only kafka:ConsumerRecord[]")
    public void testValidService4() {
        Package currentPackage = loadPackage("valid_service_4");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate kafka:Caller and kafka:ConsumerRecord[]")
    public void testValidService5() {
        Package currentPackage = loadPackage("valid_service_5");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validating readonly kafka:ConsumerRecord[]")
    public void testValidService6() {
        Package currentPackage = loadPackage("valid_service_6");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate onError function")
    public void testValidService7() {
        Package currentPackage = loadPackage("valid_service_7");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate onError function return types")
    public void testValidService8() {
        Package currentPackage = loadPackage("valid_service_8");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate data binding parameter")
    public void testValidService9() {
        Package currentPackage = loadPackage("valid_service_9");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate readonly data binding parameter")
    public void testValidService10() {
        Package currentPackage = loadPackage("valid_service_10");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate data binding parameter with @Payload annotation")
    public void testValidService11() {
        Package currentPackage = loadPackage("valid_service_11");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate `kafka:AnydataConsumerRecord` subtypes")
    public void testValidService12() {
        Package currentPackage = loadPackage("valid_service_12");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate onError method with `kafka:Error` and `kafka:Caller`")
    public void testValidService13() {
        Package currentPackage = loadPackage("valid_service_13");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 0);
    }

    @Test(enabled = true, description = "Validate no remote method")
    public void testInvalidService1() {
        Package currentPackage = loadPackage("invalid_service_1");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, NO_ON_CONSUMER_RECORD);
    }

    @Test(enabled = true, description = "Validate invalid remote method")
    public void testInvalidService2() {
        Package currentPackage = loadPackage("invalid_service_2");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, INVALID_REMOTE_FUNCTION);
    }

    @Test(enabled = true, description = "Validate onConsumerRecord without remote keyword")
    public void testInvalidService3() {
        Package currentPackage = loadPackage("invalid_service_3");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, FUNCTION_SHOULD_BE_REMOTE);
    }

    @Test(enabled = true, description = "Validate invalid parameter as the data binding param(disabled until " +
            "anydata[] parameter is allowed)")
    public void testInvalidService4() {
        Package currentPackage = loadPackage("invalid_service_4");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate invalid parameter in 2 parameter scenario")
    public void testInvalidService5() {
        Package currentPackage = loadPackage("invalid_service_5");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 5);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate invalid return type")
    public void testInvalidService6() {
        Package currentPackage = loadPackage("invalid_service_6");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, INVALID_RETURN_TYPE_ERROR_OR_NIL);
    }

    @Test(enabled = true, description = "Validate caller param in 1 parameter scenario")
    public void testInvalidService7() {
        Package currentPackage = loadPackage("invalid_service_7");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 3);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, INVALID_SINGLE_PARAMETER);
    }

    @Test(enabled = true, description = "Validate duplicate parameters(disabled until anydata[] parameter is allowed)")
    public void testInvalidService8() {
        Package currentPackage = loadPackage("invalid_service_8");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 6);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate multiple listeners")
    public void testInvalidService9() {
        Package currentPackage = loadPackage("invalid_service_9");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, INVALID_MULTIPLE_LISTENERS);
    }

    @Test(enabled = true, description = "Validate resource function")
    public void testInvalidService10() {
        Package currentPackage = loadPackage("invalid_service_10");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, PluginConstants.CompilationErrors.INVALID_RESOURCE_FUNCTION);
    }

    @Test(enabled = true, description = "Validate 0 parameter scenario")
    public void testInvalidService11() {
        Package currentPackage = loadPackage("invalid_service_11");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, MUST_HAVE_CALLER_AND_RECORDS);
    }

    @Test(enabled = true, description = "Validate readonly 2nd parameter scenario")
    public void testInvalidService12() {
        Package currentPackage = loadPackage("invalid_service_12");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate multiple caller scenario")
    public void testInvalidService13() {
        Package currentPackage = loadPackage("invalid_service_13");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate invalid parameter for onError")
    public void testInvalidService14() {
        Package currentPackage = loadPackage("invalid_service_14");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 5);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, ONLY_ERROR_ALLOWED);
        }
    }

    @Test(enabled = true, description = "Validate 0 parameter for onError")
    public void testInvalidService15() {
        Package currentPackage = loadPackage("invalid_service_15");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, MUST_HAVE_ERROR);
    }

    @Test(enabled = true, description = "Validate invalid remote function with onError")
    public void testInvalidService16() {
        Package currentPackage = loadPackage("invalid_service_16");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_REMOTE_FUNCTION);
        }
    }

    @Test(enabled = true, description = "Validate no remote keyword for onError")
    public void testInvalidService17() {
        Package currentPackage = loadPackage("invalid_service_17");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 1);
        Diagnostic diagnostic = (Diagnostic) diagnosticResult.errors().toArray()[0];
        assertDiagnostic(diagnostic, FUNCTION_SHOULD_BE_REMOTE);
    }

    @Test(enabled = true, description = "Validate resource functions with onError")
    public void testInvalidService18() {
        Package currentPackage = loadPackage("invalid_service_18");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_RESOURCE_FUNCTION);
        }
    }

    @Test(enabled = true, description = "Validate parameters more than 3 for onConsumerRecord")
    public void testInvalidService19() {
        Package currentPackage = loadPackage("invalid_service_19");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 3);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_COUNT);
        }
    }

    @Test(enabled = true, description = "Validate readonly payload parameter(disabled until " +
            "anydata[] parameter is allowed)")
    public void testInvalidService20() {
        Package currentPackage = loadPackage("invalid_service_20");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 4);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate payload parameter in 1 parameter scenario")
    public void testInvalidService21() {
        Package currentPackage = loadPackage("invalid_service_21");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 7);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_SINGLE_PARAMETER);
        }
    }

    @Test(enabled = true, description = "Validate payload parameter without @Payload flag")
    public void testInvalidService22() {
        Package currentPackage = loadPackage("invalid_service_22");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 3);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_PARAM_TYPES);
        }
    }

    @Test(enabled = true, description = "Validate object type parameters")
    public void testInvalidService23() {
        Package currentPackage = loadPackage("invalid_service_23");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, INVALID_SINGLE_PARAMETER);
        }
    }

    @Test(enabled = true, description = "Validate caller parameter in onError")
    public void testInvalidService24() {
        Package currentPackage = loadPackage("invalid_service_24");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        Assert.assertEquals(diagnosticResult.errors().size(), 2);
        Object[] diagnostics = diagnosticResult.errors().toArray();
        for (Object obj : diagnostics) {
            Diagnostic diagnostic = (Diagnostic) obj;
            assertDiagnostic(diagnostic, ONLY_CALLER_ALLOWED);
        }
    }

    private Package loadPackage(String path) {
        Path projectDirPath = RESOURCE_DIRECTORY.resolve(BALLERINA_SOURCES).resolve(path);
        BuildProject project = BuildProject.load(getEnvironmentBuilder(), projectDirPath);
        return project.currentPackage();
    }

    private void assertDiagnostic(Diagnostic diagnostic, PluginConstants.CompilationErrors error) {
        Assert.assertEquals(diagnostic.diagnosticInfo().code(), error.getErrorCode());
        Assert.assertEquals(diagnostic.diagnosticInfo().messageFormat(),
                error.getError());
    }
}
