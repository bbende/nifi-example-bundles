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
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Test for PutSolr processor.
 */
public class PutSolrTest {

    private static final String TEST_JSON = "{\"extraData\":{},\"id\":9,\"text\":\"text9\"," +
            "\"createdAt\":1422227145865,\"fromUser\":\"fromUser\",\"profileImageUrl\":\"profileImageUrl\"," +
            "\"toUserId\":9876,\"inReplyToStatusId\":null,\"inReplyToUserId\":null,\"inReplyToScreenName\":null," +
            "\"fromUserId\":12345,\"languageCode\":\"en\",\"source\":\"source\",\"retweetCount\":null," +
            "\"retweeted\":false,\"retweetedStatus\":null,\"favorited\":false,\"favoriteCount\":null," +
            "\"entities\":null,\"user\":null," +"\"unmodifiedText\":\"text9\",\"retweet\":false}\n";

    @Test
    public void testProcessor() throws IOException, SolrServerException {
        final TestableProcessor proc = new TestableProcessor();
        final TestRunner testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PutSolr.SOLR_TYPE, PutSolr.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolr.SOLR_LOCATION, "http://localhost:8443/solr");

        testRunner.enqueue(TEST_JSON.getBytes("UTF-8"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutSolr.REL_FAILURE, 0);
        testRunner.assertAllFlowFilesTransferred(PutSolr.REL_COMM_FAILURE, 0);

        verify(proc.getMockSolrServer(), times(1)).add(any(SolrInputDocument.class));
    }

    @Test
    public void testCustomValidateCloudRequiresCollection() {
        final TestRunner testRunner = TestRunners.newTestRunner(PutSolr.class);
        testRunner.setProperty(PutSolr.SOLR_TYPE, PutSolr.SOLR_TYPE_CLOUD.getValue());
        testRunner.setProperty(PutSolr.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.assertNotValid();

        testRunner.setProperty(PutSolr.COLLECTION, "someCollection1");
        testRunner.assertValid();
    }

    @Test
    public void testCustomValidateStandardDoesNotRequireCollection() {
        final TestRunner testRunner = TestRunners.newTestRunner(PutSolr.class);
        testRunner.setProperty(PutSolr.SOLR_TYPE, PutSolr.SOLR_TYPE_STANDARD.getValue());
        testRunner.setProperty(PutSolr.SOLR_LOCATION, "http://localhost:8443/solr");
        testRunner.assertValid();
    }

    /**
     * Override the creatrSolrServer method to inject a Mock.
     */
    private class TestableProcessor extends PutSolr {

        private SolrServer mockSolrServer;

        @Override
        protected SolrServer createSolrServer(ProcessContext context) {
            mockSolrServer = Mockito.mock(SolrServer.class);
            return mockSolrServer;
        }

        public SolrServer getMockSolrServer() {
            return mockSolrServer;
        }
    }

}
