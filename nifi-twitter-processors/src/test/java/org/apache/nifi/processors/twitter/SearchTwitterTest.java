package org.apache.nifi.processors.twitter;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * @author bryanbende
 */
public class SearchTwitterTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(SearchTwitter.class);
    }

    @Test
    public void testProcessor() {

    }

}
