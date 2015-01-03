package org.apache.nifi.processors.twitter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.social.twitter.api.SearchResults;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author bryanbende
 */
@Tags({"twitter", "search"})
@CapabilityDescription("Calls the Twitter Search API with the given query and outputs the results " +
        "as flow files. This processor is intended to be scheduled on a cron that executes regularly. " +
        "Each time the processor executes it searches for new results since the previous execution, " +
        "except for the first execution which grabs the most recent 100 results. As a result of tracking " +
        "the state of the previous search, this processor can only be run single-threaded.")
public class SearchTwitter extends AbstractProcessor {

    public static final PropertyDescriptor CONSUMER_KEY = new PropertyDescriptor
            .Builder().name("Consumer Key")
            .description("The Twitter application consumer key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONSUMER_SECRET = new PropertyDescriptor
            .Builder().name("Consumer Secret")
            .description("The Twitter application consumer secret")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor
            .Builder().name("Query")
            .description("The query to submit to the Twitter Search API, see " +
                    "API documentation for more details: https://dev.twitter.com/rest/public/search")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RESULTS = new Relationship.Builder()
            .name("results")
            .description("The response from the Twitter API for the given query")
            .build();

    public static final int PAGE_SIZE = 100;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private String consumerKey;
    private String consumerSecret;

    private Twitter twitter;

    private String query;

    private long prevMaxId;


    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CONSUMER_KEY);
        descriptors.add(CONSUMER_SECRET);
        descriptors.add(QUERY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RESULTS);
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
        this.consumerKey = context.getProperty(CONSUMER_KEY).getValue();
        this.consumerSecret = context.getProperty(CONSUMER_SECRET).getValue();

        this.twitter = new TwitterTemplate(consumerKey, consumerSecret);
        this.query = context.getProperty(QUERY).getValue();
        this.prevMaxId = 0;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        long totalTweets = 0;
        SearchResults results = null;

        // first time get most recent results, after that get all results since last query
        if (prevMaxId == 0) {
            results = twitter.searchOperations().search(query, PAGE_SIZE);
            for (Tweet tweet : results.getTweets()) {
                process(tweet);
                totalTweets++;
            }
        } else {
            results = twitter.searchOperations().search(query,
                    PAGE_SIZE, prevMaxId, 0);

            long currId = results.getSearchMetadata().getMaxId();

            while (results.getTweets().size() > 0) {
                getLogger().info("Got " + results.getTweets().size() + " results");

                for (Tweet tweet : results.getTweets()) {
                    process(tweet);
                    currId = tweet.getId();
                    totalTweets++;
                }

                // max_id is inclusive so use currId-1 to avoid reprocessing a result
                results = twitter.searchOperations().search(query,
                        PAGE_SIZE, prevMaxId, currId - 1);
            }
        }

        // keep track of where we should pick up next time
        prevMaxId = results.getSearchMetadata().getMaxId();

        getLogger().info("Total tweets " + totalTweets);
        getLogger().info("Picking up at " + prevMaxId + " next time...");
    }

    private void process(Tweet tweet) {
        System.out.println("tweet id: " + tweet.getId());
    }

    public Twitter getTwitter() {
        return twitter;
    }

    public void setTwitter(Twitter twitter) {
        this.twitter = twitter;
    }

}
