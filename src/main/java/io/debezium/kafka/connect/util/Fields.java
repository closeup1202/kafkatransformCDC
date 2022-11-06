package io.debezium.kafka.connect.util;

import java.util.List;

public final class Fields {

    private Fields() throws Exception {
        throw new UtilClassException();
    }

    static final String BEFORE = "before";
    static final String AFTER = "after";
    static final String SOURCE = "source";
    static final String TRANSACTION = "transaction";
    static final String OP = "op";
    static final String META = "meta";
    static final String TS_MS = "ts_ms";
    static final String KEY = "key";
    static final String DATA = "data";

    static final List<String> EXCLUDE = List.of(TRANSACTION, OP, TS_MS, SOURCE);
}
