package org.apache.nifi.processors.solr;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.solr.common.params.MultiMapSolrParams;

import java.util.HashMap;
import java.util.Map;

/**
 * @author bbende
 */
public class RequestParamsUtil {

    /**
     * Parses a String of request params into a MultiMap.
     *
     * @param requestParams
     *          the value of the request params property
     * @return
     */
    public static MultiMapSolrParams parse(final String requestParams) {
        final Map<String,String[]> paramsMap = new HashMap<>();
        if (requestParams == null || requestParams.trim().isEmpty()) {
            return new MultiMapSolrParams(paramsMap);
        }

        final String[] params = requestParams.split("[&]");
        if (params == null || params.length == 0) {
            throw new IllegalStateException(
                    "Parameters must be in form k1=v1&k2=v2, was" + requestParams);
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
     * Creates a property validator for a request params string.
     *
     * @return valid if the input parses successfully, invalid otherwise
     */
    public static Validator getValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                try {
                    RequestParamsUtil.parse(input);
                    return new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Valid Params").valid(true).build();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Invalid Params" + e.getMessage()).valid(false).build();
                }
            }
        };
    }
}
