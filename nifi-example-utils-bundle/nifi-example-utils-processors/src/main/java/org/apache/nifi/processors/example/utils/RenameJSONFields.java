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

import com.fasterxml.jackson.core.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"json,rename"})
@CapabilityDescription("A processor that can rename, or exclude, fields from a JSON document. Renaming " +
        "fields is achieved by providing a comma-separated list mappings from original field name to new " +
        "field name. Excluding fields is achieved by providing a comma separated list of field names to exclude.")
public class RenameJSONFields extends AbstractProcessor {

    public static final PropertyDescriptor FIELD_MAPPINGS = new PropertyDescriptor
            .Builder().name("Field Mappings")
            .description("A comma separated list of field mappings, i.e. a=x,b=y,c=z")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXCLUDE_FIELDS = new PropertyDescriptor
            .Builder().name("Exclude Fields")
            .description("A comma separated list of field names to exclude from the output.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The JSON with field mappings applied.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original JSON that failed renaming.")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    private Set<String> excludeFields;
    private Map<String,String> fieldMappings;


    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELD_MAPPINGS);
        descriptors.add(EXCLUDE_FIELDS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        Map<String,String> mFieldMappings = new HashMap<>();
        String fieldMappingsProp = context.getProperty(FIELD_MAPPINGS).getValue();

        String[] fieldMappingsSplit = fieldMappingsProp.split("[,]");
        for (String fieldMapping : fieldMappingsSplit) {
            int equalsIndex = fieldMapping.indexOf('=');
            String fieldName = fieldMapping.substring(0, equalsIndex);
            String mappedName = fieldMapping.substring(equalsIndex+1);
            mFieldMappings.put(fieldName, mappedName);
        }
        this.fieldMappings = Collections.unmodifiableMap(mFieldMappings);

        Set<String> mExcludeFields = new HashSet<>();
        String excludeFields = context.getProperty(EXCLUDE_FIELDS).getValue();

        if (excludeFields != null && !excludeFields.isEmpty()) {
            String[] excludeFieldsSplit = excludeFields.split("[,]");
            for (String excludeField : excludeFieldsSplit) {
                mExcludeFields.add(excludeField);
            }
        }

        this.excludeFields = Collections.unmodifiableSet(mExcludeFields);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        RenameJSONFieldsStreamCallback streamCallback = new RenameJSONFieldsStreamCallback();
        flowFile = session.write(flowFile, streamCallback);

        if (streamCallback.getError() != null) {
            getLogger().error("Failed processing JSON for {}; routing to 'failure'",
                    new Object[]{flowFile}, streamCallback.getError());
            session.transfer(flowFile, REL_FAILURE);
        } else {
            getLogger().info("Successfully processed JSON for {}; routing to 'success'",
                    new Object[]{flowFile});
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    /**
     * StreamingCallback that processes JSON and renames fields.
     */
    private class RenameJSONFieldsStreamCallback implements StreamCallback {

        private final ObjectHolder<Exception> error = new ObjectHolder<>(null);

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            JsonParser jParser = jsonFactory.createParser(in);
            JsonGenerator jGenerator = jsonFactory.createGenerator(out);

            try {
                while (jParser.nextToken() != null) {
                    JsonToken currToken = jParser.getCurrentToken();

                    switch (currToken) {
                        case START_OBJECT:
                            jGenerator.writeStartObject();
                            break;
                        case END_OBJECT:
                            jGenerator.writeEndObject();
                            break;
                        case START_ARRAY:
                            jGenerator.writeStartArray();
                            break;
                        case END_ARRAY:
                            jGenerator.writeEndArray();
                            break;
                        case FIELD_NAME:
                            String fieldName = jParser.getCurrentName();
                            if (excludeFields.contains(fieldName)) {
                                // skip the next value
                                jParser.nextToken();
                            } else {
                                if (fieldMappings.containsKey(fieldName)) {
                                    fieldName = fieldMappings.get(fieldName);
                                }
                                jGenerator.writeFieldName(fieldName);
                            }
                            break;
                        case VALUE_FALSE:
                            jGenerator.writeBoolean(false);
                            break;
                        case VALUE_TRUE:
                            jGenerator.writeBoolean(true);
                            break;
                        case VALUE_NULL:
                            jGenerator.writeNull();
                            break;
                        case VALUE_NUMBER_FLOAT:
                            jGenerator.writeNumber(jParser.getFloatValue());
                            break;
                        case VALUE_NUMBER_INT:
                            jGenerator.writeNumber(jParser.getIntValue());
                            break;
                        case VALUE_STRING:
                            jGenerator.writeString(jParser.getValueAsString());
                            break;
                        default:
                            getLogger().error("Unknown token: " + currToken.name());
                            break;
                    }
                }
            } catch (JsonParseException | JsonGenerationException e) {
                error.set(e);
            } finally {
                try {
                    jParser.close();
                } catch (Exception e) {
                }
                try {
                    jGenerator.close();
                } catch (Exception e) {
                }
            }
        }

        public Exception getError() {
            return error.get();
        }
    }

}
