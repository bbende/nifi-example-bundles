package org.apache.nifi.processors.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.social.twitter.api.SearchResults;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author bbende
 */
@Tags({"twitter", "search"})
@CapabilityDescription("Calls the Twitter Search API with the given query and emits each Tweet as a " +
        "FlowFile. The content of each FlowFile will be the JSON representation of the Tweet class from " +
        "the spring-social-twitter project. This processor is intended to be scheduled on a timer that " +
        "executes regularly. Each time the processor executes it searches for new results since the " +
        "previous execution, except for the first execution which grabs the most recent results.")
public class GetTwitterSearch extends AbstractProcessor {

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

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor
            .Builder().name("Page Size")
            .description("Number of results to get per page.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("An individual tweet in JSON format.")
            .build();


    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile Twitter twitter;
    private volatile long prevMaxId;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Lock searchLock = new ReentrantLock();

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CONSUMER_KEY);
        descriptors.add(CONSUMER_SECRET);
        descriptors.add(QUERY);
        descriptors.add(PAGE_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
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
        String consumerKey = context.getProperty(CONSUMER_KEY).getValue();
        String consumerSecret = context.getProperty(CONSUMER_SECRET).getValue();
        this.twitter = createTwitter(consumerKey, consumerSecret);
        this.prevMaxId = 0;
    }

    protected Twitter createTwitter(String consumerKey, String consumerSecret) {
        return new TwitterTemplate(consumerKey, consumerSecret);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String query = context.getProperty(QUERY).getValue();
        final Integer pageSize = context.getProperty(PAGE_SIZE).asInteger();

        try {
            // prevent another thread from changing prevMaxId
            searchLock.lock();

            // first time get most recent results
            if (prevMaxId == 0) {
                SearchResults results = twitter.searchOperations().search(query, pageSize);
                handleResults(session, results);

                // keep track of where we should pick up next time
                prevMaxId = results.getSearchMetadata().getMaxId();
            } else {
                // pick up where we left off from last time
                SearchResults results = twitter.searchOperations().search(query,
                        pageSize, prevMaxId, 0);

                // page through results...
                while (results.getTweets().size() > 0) {
                    long lastId = handleResults(session, results);

                    // max_id is inclusive so use lastId-1 to avoid reprocessing a result
                    results = twitter.searchOperations().search(query,
                            pageSize, prevMaxId, lastId - 1);
                }

                // keep track of where we should pick up next time
                prevMaxId = results.getSearchMetadata().getMaxId();
            }
        } finally {
            searchLock.unlock();
        }

        getLogger().info("Picking up at " + prevMaxId + " next time...");
    }

    /**
     * Emits a FlowFile for each Tweet in the SearchResults, the FlowFile content
     * will be the Tweet in JSON format.
     *
     * @param session
     * @param results
     * @return the id of the last Tweet in this page of results
     */
    protected long handleResults(ProcessSession session, SearchResults results) {
        long lastId = 0;

        for (final Tweet tweet : results.getTweets()) {
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    mapper.writeValue(out, tweet);
                }
            });

            session.transfer(flowFile, SUCCESS);
            lastId = tweet.getId();
        }

        return lastId;
    }

}
