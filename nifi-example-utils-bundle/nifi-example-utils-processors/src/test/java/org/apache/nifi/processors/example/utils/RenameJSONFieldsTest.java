/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.example.utils;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


public class RenameJSONFieldsTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(RenameJSONFields.class);
    }

    @Test
    public void testRenameFlatDocument() throws IOException {
        final String inputJson = "{" +
                        "\"field1\":123," +
                        "\"field2\":\"abc\"," +
                        "\"field3\":\"xyz\"" +
                        "}";

        final String fieldMappings = "field1=fieldA,field3=fieldB";
        final String expectedJson =
                "{\"fieldA\":123,\"field2\":\"abc\"," +
                        "\"fieldB\":\"xyz\"}";

        testRunner.setProperty(RenameJSONFields.FIELD_MAPPINGS, fieldMappings);

        testRunner.enqueue(inputJson.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RenameJSONFields.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(
                RenameJSONFields.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertContentEquals(expectedJson);
    }

    @Test
    public void testRenameComplexDocument() throws IOException {
        final String inputJson = "{" +
                "\"field1\":123," +
                "\"field2\":[" +
                    "{\"field11\":11}," +
                    "{\"field22\":22}" +
                "]," +
                "\"field3\":\"xyz\"" +
                "}";

        final String fieldMappings = "field11=fieldXX,field3=fieldB";

        final String expectedJson = "{" +
                "\"field1\":123," +
                "\"field2\":[" +
                    "{\"fieldXX\":11}," +
                    "{\"field22\":22}" +
                "]," +
                "\"fieldB\":\"xyz\"" +
                "}";

        testRunner.setProperty(RenameJSONFields.FIELD_MAPPINGS, fieldMappings);

        testRunner.enqueue(inputJson.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RenameJSONFields.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(
                RenameJSONFields.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertContentEquals(expectedJson);
    }

    @Test
    public void testExcludeFirstField() throws IOException {
        final String inputJson = "{" +
                "\"field1\":123," +
                "\"field2\":\"abc\"," +
                "\"field3\":\"xyz\"" +
                "}";

        final String fieldMappings = "field1=fieldA,field3=fieldB";
        final String excludeFields = "field1";

        final String expectedJson =
                "{\"field2\":\"abc\",\"fieldB\":\"xyz\"}";

        testRunner.setProperty(RenameJSONFields.FIELD_MAPPINGS, fieldMappings);
        testRunner.setProperty(RenameJSONFields.EXCLUDE_FIELDS, excludeFields);

        testRunner.enqueue(inputJson.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RenameJSONFields.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(
                RenameJSONFields.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertContentEquals(expectedJson);
    }

    @Test
    public void testExcludeMultipleFields() throws IOException {
        final String inputJson = "{" +
                "\"field1\":123," +
                "\"field2\":\"abc\"," +
                "\"field3\":\"xyz\"" +
                "}";

        final String fieldMappings = "field1=fieldA,field3=fieldB";
        final String excludeFields = "field1,field3";

        final String expectedJson =
                "{\"field2\":\"abc\"}";

        testRunner.setProperty(RenameJSONFields.FIELD_MAPPINGS, fieldMappings);
        testRunner.setProperty(RenameJSONFields.EXCLUDE_FIELDS, excludeFields);

        testRunner.enqueue(inputJson.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RenameJSONFields.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(
                RenameJSONFields.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertContentEquals(expectedJson);
    }

    @Test
    public void testInvalidJson() throws IOException {
        final String inputJson = "{field:,, }";

        final String fieldMappings = "field1=fieldA,field3=fieldB";
        final String excludeFields = "field1,field3";

        testRunner.setProperty(RenameJSONFields.FIELD_MAPPINGS, fieldMappings);
        testRunner.setProperty(RenameJSONFields.EXCLUDE_FIELDS, excludeFields);

        testRunner.enqueue(inputJson.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(RenameJSONFields.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(
                RenameJSONFields.REL_FAILURE);
        Assert.assertEquals(1, flowFiles.size());
    }

}
