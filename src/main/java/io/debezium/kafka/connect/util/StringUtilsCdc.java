package io.debezium.kafka.connect.util;

import org.apache.commons.lang3.StringUtils;

public final class StringUtilsCdc extends StringUtils {

    private StringUtilsCdc() throws Exception {
        throw new UtilClassException();
    }

    private static final String REGEX = ",";

    public static String[] subStringAndSplit(final String str){
        return deleteWhitespace(substring(str, 1, -1)).split(REGEX);
    }

}
