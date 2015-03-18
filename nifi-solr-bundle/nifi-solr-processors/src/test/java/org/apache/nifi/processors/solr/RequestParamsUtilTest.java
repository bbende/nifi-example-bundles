package org.apache.nifi.processors.solr;

import org.junit.Assert;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.junit.Test;

/**
 * @author bbende
 */
public class RequestParamsUtilTest {

    @Test
    public void testSimpleParse() {
        MultiMapSolrParams map = RequestParamsUtil.parse("a=1&b=2&c=3");
        Assert.assertEquals("1", map.get("a"));
        Assert.assertEquals("2", map.get("b"));
        Assert.assertEquals("3", map.get("c"));
    }

    @Test
    public void testParseWithSpaces() {
        MultiMapSolrParams map = RequestParamsUtil.parse("a = 1 &b= 2& c= 3 ");
        Assert.assertEquals("1", map.get("a"));
        Assert.assertEquals("2", map.get("b"));
        Assert.assertEquals("3", map.get("c"));
    }

    @Test(expected = IllegalStateException.class)
    public void testMalformedParamsParse() {
        RequestParamsUtil.parse("a=1&b&c=3");
    }

}
