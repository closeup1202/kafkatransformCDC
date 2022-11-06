package io.debezium.kafka.connect.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public final class DateFormatCdc {

    private DateFormatCdc() throws Exception {
        throw new UtilClassException();
    }

    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(PATTERN);

    static String convert(Object timeStamp){
        long dateTime = Long.parseLong(timeStamp.toString());
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime), TimeZone.getDefault().toZoneId())
                            .format(dateTimeFormatter);
    }
}
