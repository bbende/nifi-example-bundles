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
package org.apache.nifi.processors.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
public class PutSolr extends AbstractProcessor {

    public static final AllowableValue SOLR_TYPE_CLOUD = new AllowableValue(
            "0", "Cloud", "A SolrCloud instance.");

    public static final AllowableValue SOLR_TYPE_STANDARD = new AllowableValue(
            "1", "Standard", "A stand-alone Solr instance.");

    public static final PropertyDescriptor SOLR_TYPE = new PropertyDescriptor
            .Builder().name("Solr Type")
            .description("The type of Solr instance, Cloud or Standard.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(SOLR_TYPE_CLOUD, SOLR_TYPE_STANDARD)
            .defaultValue(SOLR_TYPE_STANDARD.getValue())
            .build();

    public static final PropertyDescriptor SOLR_LOCATION = new PropertyDescriptor
            .Builder().name("Solr Location")
            .description("The Solr url for a Solr Type of Standard, " +
                    "or the ZooKeeper hosts for a Solr Type of Cloud.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLLECTION = new PropertyDescriptor
            .Builder().name("Default Collection")
            .description("The Solr collection name, only used with a Solr Type of Cloud")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .description("The number of documents to batch in a single add operation.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure to process due to some kind of error.")
            .build();

    public static final Relationship REL_COMM_FAILURE = new Relationship.Builder()
            .name("communication failure")
            .description("Failure to process due to an error connecting to Solr.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private SolrServer solrServer;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_COMM_FAILURE);
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

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(
                super.customValidate(context));

        String solrType = context.getProperty(SOLR_TYPE).getValue();
        if (SOLR_TYPE_CLOUD.getValue().equals(solrType)) {
            String collection = context.getProperty(COLLECTION).getValue();
            if (collection == null || collection.trim().isEmpty()) {
                problems.add(new ValidationResult.Builder()
                        .subject(COLLECTION.getName())
                        .input(collection)
                        .valid(false)
                        .explanation("A collection must specified for Solr Cloud")
                        .build());

            }
        }

        return problems;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.solrServer = createSolrServer(context);
    }

    protected SolrServer createSolrServer(final ProcessContext context) {
        String solrType = context.getProperty(SOLR_TYPE).getValue();
        String solrLocation = context.getProperty(SOLR_LOCATION).getValue();
        if (solrType.equals(SOLR_TYPE_STANDARD.getValue())) {
            return new HttpSolrServer(solrLocation);
        } else {
            String defaultCollection = context.getProperty(COLLECTION).getValue();
            CloudSolrServer cloudSolrServer = new CloudSolrServer(solrLocation);
            cloudSolrServer.setDefaultCollection(defaultCollection);
            return cloudSolrServer;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

        final byte[] value = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, value);
            }
        });

        try {
            Map<String,Object> values = mapper.readValue(value, Map.class);
            SolrInputDocument solrDoc = getSolrInputDocument(values);
            if (solrDoc != null) {
                solrServer.add(solrDoc);
            } else {
                getLogger().info("No values, nothing to add to Solr");
            }
            session.remove(flowFile);
        } catch (SolrServerException e) {
            getLogger().error("Error adding documents to Solr", e);
            session.transfer(flowFile, REL_COMM_FAILURE);
        } catch (Exception e) {
            getLogger().error("Error processing FlowFile", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private SolrInputDocument getSolrInputDocument(Map<String,Object> values) {
        if (values != null && values.size() > 0) {
            SolrInputDocument solrDoc = new SolrInputDocument();
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                if (entry.getValue() != null) {
                    solrDoc.addField(entry.getKey(), entry.getValue());
                }
            }
            return solrDoc;
        } else {
            return null;
        }
    }

}
