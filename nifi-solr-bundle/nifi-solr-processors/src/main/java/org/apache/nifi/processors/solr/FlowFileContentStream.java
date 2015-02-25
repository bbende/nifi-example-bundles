package org.apache.nifi.processors.solr;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.ObjectHolder;
import org.apache.solr.common.util.ContentStreamBase;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author bryanbende
 */
public class FlowFileContentStream extends ContentStreamBase {

    private final FlowFile flowFile;

    private final ProcessSession session;

    public FlowFileContentStream(FlowFile flowFile, ProcessSession session) {
        this.flowFile = flowFile;
        this.session = session;
    }

    @Override
    public InputStream getStream() throws IOException {
        final ObjectHolder<InputStream> inputStreamHolder = new ObjectHolder<>(null);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                inputStreamHolder.set(in);
            }
        });
        return inputStreamHolder.get();
    }

}
