package org.apache.nifi.processors.twitter;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.social.twitter.api.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test for the GetTwitterSearchProcessor.
 *
 * @author bbende
 */
public class GetTwitterSearchTest {

    private Logger LOGGER = LoggerFactory.getLogger(GetTwitterSearchTest.class);

    @Test
    public void testGetFirstResults() {
        final String query = "test query";
        final int pageSize = 5;

        // make some fake Tweets to return
        List<Tweet> tweets = getTweets(10);
        SearchMetadata metaData = new SearchMetadata(123456789,123456789);
        SearchResults results = new SearchResults(tweets, metaData);

        final TestableProcessor proc = new TestableProcessor(query, pageSize, results);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(GetTwitterSearch.CONSUMER_KEY, "testKey");
        runner.setProperty(GetTwitterSearch.CONSUMER_SECRET, "testSecret");
        runner.setProperty(GetTwitterSearch.QUERY, query);
        runner.setProperty(GetTwitterSearch.PAGE_SIZE, Integer.toString(pageSize));

        // run the processor twice to test picking up from previous spot
        runner.run(2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(
                GetTwitterSearch.SUCCESS);

        Assert.assertEquals(2 * tweets.size(), flowFiles.size());

        for ( final MockFlowFile flowFile : flowFiles ) {
            LOGGER.debug(flowFile.getAttributes().toString());
            LOGGER.debug(new String(flowFile.toByteArray()));
        }
    }

    private List<Tweet> getTweets(int num) {
        List<Tweet> tweets = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            tweets.add(
                    new Tweet(i, "text" + i, new Date(), "fromUser",
                            "profileImageUrl", 9876L, 12345L, "en", "source"));
        }
        return tweets;
    }

    /**
     * Extends the real processor to mock the Twitter client.
     */
    private class TestableProcessor extends GetTwitterSearch {

        private final String query;
        private final Integer pageSize;
        private final SearchResults results;

        public TestableProcessor(String query, Integer pageSize, SearchResults results) {
            this.query = query;
            this.pageSize = pageSize;
            this.results = results;
        }

        @Override
        protected Twitter createTwitter(String consumerKey, String consumerSecret) {
            long maxId = 0;
            long lastId = 8;
            long prevMaxId = results.getSearchMetadata().getMaxId();

            Twitter twitter = Mockito.mock(Twitter.class);
            SearchOperations searchOperations = Mockito.mock(SearchOperations.class);
            when(twitter.searchOperations()).thenReturn(searchOperations);

            // first call when there is no previous id
            when(searchOperations.search(eq(query), eq(pageSize)))
                    .thenReturn(results);

            // second call when there is a prev id
            when(searchOperations.search(eq(query), eq(pageSize),
                    eq(prevMaxId), eq(maxId)))
                    .thenReturn(results);

            // part of second call when next page is requested
            when(searchOperations.search(eq(query), eq(pageSize),
                    eq(prevMaxId), eq(lastId)))
                    .thenReturn(
                            new SearchResults(
                                    new ArrayList<Tweet>(),
                                    new SearchMetadata(0,0))
                    );

            return twitter;
        }

    }
}
