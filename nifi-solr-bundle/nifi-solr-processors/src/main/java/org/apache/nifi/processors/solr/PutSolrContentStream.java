/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStreamBase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"Apache", "Solr", "Put", "Send"})
@CapabilityDescription("Sends the contents of a FlowFile as a ContentStream to Solr")
public class PutSolrContentStream extends SolrProcessor {

    public static final PropertyDescriptor CONTENT_STREAM_URL = new PropertyDescriptor
            .Builder().name("Content Stream URL")
            .description("The URL in Solr to post the ContentStream")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("/update/json/docs")
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor
            .Builder().name("Content-Type")
            .description("Content-Type being sent to Solr")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("application/json")
            .build();

    public static final PropertyDescriptor REQUEST_PARAMS = new PropertyDescriptor
            .Builder().name("Request Parameters")
            .description("Additional parameters to pass to Solr on each request, i.e. key1=val1&key2=val2")
            .required(false)
            .addValidator(RequestParamsUtil.getValidator())
            .defaultValue("json.command=false&split=/&f=id:/field1")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed for any reason other than Solr being unreachable")
            .build();

    public static final Relationship REL_CONNECTION_FAILURE = new Relationship.Builder()
            .name("connection_failure")
            .description("FlowFiles that failed because Solr is unreachable")
            .build();

    /**
     * The name of a FlowFile attribute used for specifying a Solr collection.
     */
    public static final String SOLR_COLLECTION_ATTR = "solr.collection";

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private volatile MultiMapSolrParams requestParams;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(DEFAULT_COLLECTION);
        descriptors.add(CONTENT_STREAM_URL);
        descriptors.add(CONTENT_TYPE);
        descriptors.add(REQUEST_PARAMS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_CONNECTION_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected void additionalOnScheduled(ProcessContext context) {
        final String requestParamsVal = context.getProperty(REQUEST_PARAMS).getValue();
        this.requestParams = RequestParamsUtil.parse(requestParamsVal);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final ObjectHolder<SolrException> error = new ObjectHolder<>(null);
        final ObjectHolder<SolrServerException> connectionError = new ObjectHolder<>(null);
        final ObjectHolder<String> collectionUsed = new ObjectHolder<>(null);

        final String collectionAttrVal = flowFile.getAttribute(SOLR_COLLECTION_ATTR);
        final boolean isSolrCloud = SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue());

        StopWatch timer = new StopWatch(true);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                        context.getProperty(CONTENT_STREAM_URL).getValue());
                request.setParams(new ModifiableSolrParams());

                // add the extra params, don't use 'set' in case of repeating params
                Iterator<String> paramNames = requestParams.getParameterNamesIterator();
                while (paramNames.hasNext()) {
                    String paramName = paramNames.next();
                    for (String paramValue : requestParams.getParams(paramName)) {
                        request.getParams().add(paramName, paramValue);
                    }
                }

                // send the request to the specified collection, or to the default collection
                if (isSolrCloud) {
                    String collection = collectionAttrVal;
                    if (StringUtils.isBlank(collection)) {
                        collection = context.getProperty(DEFAULT_COLLECTION).getValue();
                    }
                    request.setParam("collection", collection);
                    collectionUsed.set(collection);
                }

                try (final BufferedInputStream bufferedIn = new BufferedInputStream(in)) {
                    // add the FlowFile's content on the UpdateRequest
                    request.addContentStream(new ContentStreamBase() {
                        @Override
                        public InputStream getStream() throws IOException {
                            return bufferedIn;
                        }

                        @Override
                        public String getContentType() {
                            return context.getProperty(CONTENT_TYPE).getValue();
                        }
                    });

                    UpdateResponse response = request.process(getSolrServer());
                    getLogger().debug("Got {} response from Solr", new Object[]{response.getStatus()});
                } catch (SolrException e) {
                    error.set(e);
                } catch (SolrServerException e) {
                    connectionError.set(e);
                }
            }
        });
        timer.stop();

        if (error.get() != null) {
            getLogger().error("Failed to send {} to Solr due to {} with status code {}; routing to failure",
                    new Object[]{flowFile, error.get(), error.get().code()});
            session.transfer(flowFile, REL_FAILURE);
        } else if (connectionError.get() != null) {
            getLogger().error("Failed to send {} to Solr due to {}; routing to connection_failure",
                    new Object[]{flowFile, connectionError.get()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_CONNECTION_FAILURE);
        } else {
            StringBuilder transitUri = new StringBuilder("solr://");
            transitUri.append(context.getProperty(SOLR_LOCATION).getValue());
            if (isSolrCloud) {
                transitUri.append(":").append(collectionUsed.get());
            }

            final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
            session.getProvenanceReporter().send(flowFile, transitUri.toString(), duration, true);
            getLogger().info("Successfully sent {} to Solr in {} millis", new Object[]{flowFile, duration});
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }

}
