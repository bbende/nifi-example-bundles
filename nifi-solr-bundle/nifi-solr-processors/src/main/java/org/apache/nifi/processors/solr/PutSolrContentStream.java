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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.BinFiles;
import org.apache.nifi.processors.standard.util.Bin;
import org.apache.nifi.processors.standard.util.BinManager;
import org.apache.nifi.processors.standard.util.FlowFileSessionWrapper;
import org.apache.nifi.util.ObjectHolder;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
public class PutSolrContentStream extends BinFiles {

    public static final AllowableValue SOLR_TYPE_CLOUD = new AllowableValue(
            "Cloud", "Cloud", "A SolrCloud instance.");

    public static final AllowableValue SOLR_TYPE_STANDARD = new AllowableValue(
            "Standard", "Standard", "A stand-alone Solr instance.");

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

    public static final PropertyDescriptor DEFAULT_COLLECTION = new PropertyDescriptor
            .Builder().name("Default Collection")
            .description("The Solr collection name, only used with a Solr Type of Cloud")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_STREAM_URL = new PropertyDescriptor
            .Builder().name("Content Stream URL")
            .description("The URL to post the ContentStream to in Solr.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("/update/json/docs")
            .build();

    public static final PropertyDescriptor REQUEST_PARAMS = new PropertyDescriptor
            .Builder().name("Request Parameters")
            .description("Additional parameters to pass to Solr on each request, i.e. key1=val1&key2=val2")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("json.command=false&split=/&f=id:/field1")
            .build();

    /**
     * The default group id used to bin FlowFiles in standard mode.
     */
    public static final String DEFAULT_GROUP_ID = "solr";

    /**
     * The name of a FlowFile attribute used for specifying a Solr collection.
     */
    public static final String SOLR_COLLECTION_ATTR = "solr.collection";


    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile SolrServer solrServer;
    private volatile MultiMapSolrParams requestParams;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(DEFAULT_COLLECTION);
        descriptors.add(CONTENT_STREAM_URL);
        descriptors.add(REQUEST_PARAMS);
        descriptors.add(MIN_ENTRIES);
        descriptors.add(MAX_ENTRIES);
        descriptors.add(MIN_SIZE);
        descriptors.add(MAX_SIZE);
        descriptors.add(MAX_BIN_AGE);
        descriptors.add(MAX_BIN_COUNT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();

        if (SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            final String collection = context.getProperty(DEFAULT_COLLECTION).getValue();
            if (collection == null || collection.trim().isEmpty()) {
                problems.add(new ValidationResult.Builder()
                        .subject(DEFAULT_COLLECTION.getName())
                        .input(collection).valid(false)
                        .explanation("A collection must specified for Solr Type of Cloud")
                        .build());
            }
        }
        return problems;
    }

    @Override
    protected void setUpBinManager(BinManager binManager, ProcessContext context) {
        this.solrServer = createSolrServer(context);
        this.requestParams = createRequestParams(context);
    }

    /**
     * Parse the REQUEST_PARAMS property into a MultiMap.
     *
     * @param context
     * @return
     */
    protected MultiMapSolrParams createRequestParams(final ProcessContext context) {
        final Map<String,String[]> paramsMap = new HashMap<>();
        final String requestParamsVal = context.getProperty(REQUEST_PARAMS).getValue();

        final String[] params = requestParamsVal.split("[&]");
        if (params == null || params.length == 0) {
            throw new IllegalStateException(
                    "Parameters must be in form k1=v1&k2=v2, was" + requestParamsVal);
        }

        for (final String param : params) {
            final String[] keyVal = param.split("=");
            if (keyVal.length != 2) {
                throw new IllegalStateException(
                        "Parameter must be in form key=value, was " + param);
            }

            final String key = keyVal[0].trim();
            final String val = keyVal[1].trim();
            MultiMapSolrParams.addParam(key, val, paramsMap);
        }

        return new MultiMapSolrParams(paramsMap);
    }

    /**
     * Create a SolrServer based on the type of Solr specified.
     *
     * @param context
     * @return
     */
    protected SolrServer createSolrServer(final ProcessContext context) {
        if (SOLR_TYPE_STANDARD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            return new HttpSolrServer(context.getProperty(SOLR_LOCATION).getValue());
        } else {
            CloudSolrServer cloudSolrServer = new CloudSolrServer(
                    context.getProperty(SOLR_LOCATION).getValue());
            cloudSolrServer.setDefaultCollection(
                    context.getProperty(DEFAULT_COLLECTION).getValue());
            return cloudSolrServer;
        }
    }

    @Override
    protected FlowFile preprocessFlowFile(ProcessContext processContext, ProcessSession processSession,
            FlowFile flowFile) {
        return flowFile;
    }

    @Override
    protected String getGroupId(ProcessContext context, FlowFile flowFile) {
        if (SOLR_TYPE_STANDARD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            return DEFAULT_GROUP_ID;
        } else {
            final String flowFileCollection = flowFile.getAttribute(SOLR_COLLECTION_ATTR);
            if (flowFileCollection == null || flowFileCollection.trim().isEmpty()) {
                return context.getProperty(DEFAULT_COLLECTION).getValue();
            } else {
                return flowFileCollection;
            }
        }
    }

    @Override
    protected boolean processBin(Bin bin, List<FlowFileSessionWrapper> flowFileSessionWrappers,
            ProcessContext context, ProcessSession session)
            throws ProcessException {

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

        final ObjectHolder<String> collectionHolder = new ObjectHolder<String>(null);
        final boolean isSolrCloud = SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue());

        // create a ContentStream for each FlowFile...
        for (FlowFileSessionWrapper wrapper : flowFileSessionWrappers) {
            final ContentStream contentStream = new FlowFileContentStream(
                    wrapper.getFlowFile(), wrapper.getSession());
            request.addContentStream(contentStream);

            // with SolrCloud see if the FlowFiles have a collection attribute
            if (isSolrCloud) {
                final String collectionAttrVal = wrapper.getFlowFile()
                        .getAttribute(SOLR_COLLECTION_ATTR);
                if (collectionAttrVal != null && !collectionAttrVal.trim().isEmpty()) {
                    collectionHolder.set(collectionAttrVal);
                }
            }
        }

        // send the request to the specified collection, or to the default collection
        if (isSolrCloud) {
            final String collection = collectionHolder.get() == null ?
                    context.getProperty(DEFAULT_COLLECTION).getValue() :
                    collectionHolder.get();
            request.setParam("collection", collection);
        }

        try {
            UpdateResponse response = request.process(solrServer);
            getLogger().debug("Completed UpdateRequest in {} time with status code {}",
                    new Object[] { response.getElapsedTime(), response.getStatus()});
        } catch (SolrServerException | IOException e) {
            // throw ProcessException so the parent can handle routing to failure
            throw new ProcessException(e);
        }

        // if the update request succeeded then remove FlowFiles and commit each session
        for (final FlowFileSessionWrapper wrapper : flowFileSessionWrappers) {
            wrapper.getSession().remove(wrapper.getFlowFile());
            wrapper.getSession().commit();
        }

        return true;
    }

}
