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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Test for PutSolr processor.
 */
public class PutSolrContentStreamTest {

    private static final String TEST_JSON1 = "{\"first\":\"bob\",\"last\":\"smith\"}";
    private static final String TEST_JSON2 = "{\"first\":\"mike\",\"last\":\"jones\"}";

    @Test
    public void testEmbeddedSolrServerJsonUpdateShouldSucceed() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor();

        final TestRunner testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PutSolrContentStream.SOLR_TYPE,
                PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.setProperty(PutSolrContentStream.CONTENT_STREAM_URL, "/update/json/docs");
        testRunner.setProperty(PutSolrContentStream.REQUEST_PARAMS,
                "json.command=false&split=/&f=first:/first&f=last:/last");
        testRunner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        testRunner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        testRunner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        testRunner.enqueue(TEST_JSON2.getBytes("UTF-8"));

        try {
            testRunner.run();
        } finally {
            try {
                proc.getSolrServer().shutdown();
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }

        testRunner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 0);

        // TODO query solr and see if we have two docs
    }

    @Test
    public void testJsonUpdateShouldSucceed() throws IOException, SolrServerException {
        final NamedList<Object> response = new NamedList<>();
        response.add("status", 200);

        final MockSolrServerProcessor proc = new MockSolrServerProcessor(response);
        final TestRunner testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PutSolrContentStream.SOLR_TYPE,
                PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.setProperty(PutSolrContentStream.CONTENT_STREAM_URL, "/update/json/docs");
        testRunner.setProperty(PutSolrContentStream.REQUEST_PARAMS,
                "json.command=false&split=\"/\"&f=\"id:/field1\"");
        testRunner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        testRunner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        testRunner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        testRunner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 0);
        verify(proc.getSolrServer(), times(1)).request(any(SolrRequest.class));

        // TODO verify the correct request was produced
    }

    @Test
    public void testJsonUpdateShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrServerException("Error adding docs");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PutSolrContentStream.SOLR_TYPE,
                PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.setProperty(PutSolrContentStream.CONTENT_STREAM_URL, "/update/json/docs");
        testRunner.setProperty(PutSolrContentStream.REQUEST_PARAMS,
                "json.command=false&split=\"/\"&f=\"id:/field1\"");
        testRunner.setProperty(PutSolrContentStream.MAX_ENTRIES, "2");
        testRunner.setProperty(PutSolrContentStream.MAX_BIN_AGE, "5 seconds");

        testRunner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        testRunner.enqueue(TEST_JSON1.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 2);
        verify(proc.getSolrServer(), times(1)).request(any(SolrRequest.class));
    }

    @Test
    public void testCustomValidateCloudRequiresCollection() {
        final TestRunner testRunner = TestRunners.newTestRunner(PutSolrContentStream.class);
        testRunner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_CLOUD.getValue());
        testRunner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.assertNotValid();

        testRunner.setProperty(PutSolrContentStream.DEFAULT_COLLECTION, "someCollection1");
        testRunner.assertValid();
    }

    @Test
    public void testCustomValidateStandardDoesNotRequireCollection() {
        final TestRunner testRunner = TestRunners.newTestRunner(PutSolrContentStream.class);
        testRunner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.assertValid();
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

        private SolrServer embeddedSolrServer;

        @Override
        protected SolrServer createSolrServer(ProcessContext context) {
            try {
                String relPath = getClass().getProtectionDomain()
                        .getCodeSource().getLocation().getFile()
                        + "../../target";

                embeddedSolrServer = EmbeddedSolrServerFactory.create(
                        EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                        EmbeddedSolrServerFactory.DEFAULT_CORE_HOME,
                        "jsonCollection", relPath);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return embeddedSolrServer;
        }

        public SolrServer getSolrServer() {
            return embeddedSolrServer;
        }
    }

}
