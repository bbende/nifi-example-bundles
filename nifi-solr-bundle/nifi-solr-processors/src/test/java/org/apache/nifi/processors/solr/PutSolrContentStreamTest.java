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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.*;

/**
 * Test for PutSolr processor.
 */
public class PutSolrContentStreamTest {

    static final String TEST_JSON1 = "{\"first\":\"bob\",\"last\":\"smith\"}";
    static final String TEST_JSON2 = "{\"first\":\"mike\",\"last\":\"jones\"}";

    static final String DEFAULT_SOLR_CORE = "testCollection";
    static final String DEFAULT_JSON_REQUEST_PARAMS = "json.command=false&split=/&f=first:/first&f=last:/last";

    /**
     * Creates a base TestRunner with Solr Type of standard and json update path.
     */
    private static TestRunner createDefaultJsonTestRunner(PutSolrContentStream processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_URL, "/update/json/docs");
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS, DEFAULT_JSON_REQUEST_PARAMS);
        return runner;
    }

    @Test
    public void testEmbeddedSolrServerSimpleJsonUpdate() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor(DEFAULT_SOLR_CORE);

        final TestRunner runner = createDefaultJsonTestRunner(proc);
        runner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        runner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.enqueue(TEST_JSON2.getBytes("UTF-8"));

        try {
            // first run an verify there are no failures
            runner.run();
            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 0);

            // now verify the correct documents were indexed
            final SolrDocument doc1 = new SolrDocument();
            doc1.addField("first", "mike");
            doc1.addField("last", "jones");

            final SolrDocument doc2 = new SolrDocument();
            doc1.addField("first", "bob");
            doc1.addField("last", "smith");

            final Collection<SolrDocument> expectedDocuments = Collections.unmodifiableList(
                    Arrays.asList(doc1, doc2));
            verifySolrDocuments(proc.getSolrServer(), expectedDocuments);
        } finally {
            try {
                proc.getSolrServer().shutdown();
            } catch (Exception e) { }
        }
    }

    @Test
    public void testSimpleJsonUpdate() throws IOException, SolrServerException {
        final NamedList<Object> response = new NamedList<>();
        response.add("status", 200);

        final MockSolrServerProcessor proc = new MockSolrServerProcessor(response);

        final TestRunner runner = createDefaultJsonTestRunner(proc);
        runner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        runner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 0);
        verify(proc.getSolrServer(), times(1)).request(any(SolrRequest.class));

        // TODO verify the correct request was produced
    }

    @Test
    public void testSolrServerExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrServerException("Error adding docs");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultJsonTestRunner(proc);
        runner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        runner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 2);
        verify(proc.getSolrServer(), times(1)).request(any(SolrRequest.class));
    }

    @Test
    public void testSolrTypeCloudShouldRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertNotValid();

        runner.setProperty(PutSolrContentStream.DEFAULT_COLLECTION, "someCollection1");
        runner.assertValid();
    }

    @Test
    public void testSolrTypeStandardShouldNotRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();
    }

    @Test
    public void testOnlyOneRequestParam() throws IOException, SolrServerException {
        final NamedList<Object> response = new NamedList<>();
        response.add("status", 200);

        final MockSolrServerProcessor proc = new MockSolrServerProcessor(response);

        final TestRunner runner = createDefaultJsonTestRunner(proc);
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS, "a=1");
        runner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        runner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 0);
        verify(proc.getSolrServer(), times(1)).request(any(SolrRequest.class));
    }

    @Test
    public void testMalformedRequestParam() throws IOException, SolrServerException {
        final NamedList<Object> response = new NamedList<>();
        response.add("status", 200);

        final MockSolrServerProcessor proc = new MockSolrServerProcessor(response);

        final TestRunner runner = createDefaultJsonTestRunner(proc);
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS, "a=1&b");
        runner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        runner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        runner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        //runner.run();

        // TODO use a regex validator to check request params and update this test
    }


    /**
     * Override the creatrSolrServer method to inject a Mock.
     */
    private class MockSolrServerProcessor extends PutSolrContentStream {

        private SolrServer mockSolrServer;
        private NamedList<Object> response;

        public MockSolrServerProcessor(NamedList<Object> response) {
            this.response = response;
        }

        @Override
        protected SolrServer createSolrServer(ProcessContext context) {
            mockSolrServer = Mockito.mock(SolrServer.class);
            if (response != null) {
                try {
                    when(mockSolrServer.request(any(SolrRequest.class))).thenReturn(response);
                } catch (SolrServerException e) {
                    Assert.fail(e.getMessage());
                } catch (IOException e) {
                    Assert.fail(e.getMessage());
                }
            }
            return mockSolrServer;
        }

        public SolrServer getSolrServer() {
            return mockSolrServer;
        }
    }

    /**
     * Override the creatrSolrServer method to inject a Mock.
     */
    private class ExceptionThrowingProcessor extends PutSolrContentStream {

        private SolrServer mockSolrServer;
        private Throwable throwable;

        public ExceptionThrowingProcessor(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        protected SolrServer createSolrServer(ProcessContext context) {
            mockSolrServer = Mockito.mock(SolrServer.class);
            try {
                when(mockSolrServer.request(any(SolrRequest.class))).thenThrow(throwable);
            } catch (SolrServerException e) {
                Assert.fail(e.getMessage());
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return mockSolrServer;
        }

        public SolrServer getSolrServer() {
            return mockSolrServer;
        }
    }

    /**
     * Override the createSolrServer method and create and EmbeddedSolrServer.
     */
    private class EmbeddedSolrServerProcessor extends PutSolrContentStream {

        private String coreName;
        private SolrServer embeddedSolrServer;

        public EmbeddedSolrServerProcessor(String coreName) {
            this.coreName = coreName;
        }

        @Override
        protected SolrServer createSolrServer(ProcessContext context) {
            try {
                String relPath = getClass().getProtectionDomain()
                        .getCodeSource().getLocation().getFile()
                        + "../../target";

                embeddedSolrServer = EmbeddedSolrServerFactory.create(
                        EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                        EmbeddedSolrServerFactory.DEFAULT_CORE_HOME,
                        coreName, relPath);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return embeddedSolrServer;
        }

        public SolrServer getSolrServer() {
            return embeddedSolrServer;
        }
    }

    /**
     * Verify that given SolrServer contains the expected SolrDocuments.
     */
    private static void verifySolrDocuments(SolrServer solrServer,
            Collection<SolrDocument> expectedDocuments)
            throws IOException, SolrServerException {

        solrServer.commit();

        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qResponse = solrServer.query(query);
        Assert.assertEquals(2, qResponse.getResults().getNumFound());

        // verify documents have expected fields and values
        for (SolrDocument expectedDoc : expectedDocuments) {
            boolean found = false;
            for (SolrDocument solrDocument : qResponse.getResults()) {
                boolean foundAllFields = true;
                for (String expectedField : expectedDoc.getFieldNames()) {
                    Object expectedVal = expectedDoc.getFirstValue(expectedField);
                    Object actualVal = solrDocument.getFirstValue(expectedField);
                    foundAllFields = expectedVal.equals(actualVal);
                }

                if (foundAllFields) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }
    }

}
