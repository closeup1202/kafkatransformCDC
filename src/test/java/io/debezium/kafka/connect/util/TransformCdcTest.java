package io.debezium.kafka.connect.util;

import io.debezium.ConverterCdcApplicationTests;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

import static io.debezium.kafka.connect.util.TestSchemas.*;
import static io.debezium.kafka.connect.util.TestValues.*;

@SpringBootTest(classes = ConverterCdcApplicationTests.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TransformCdcTest {

    @AfterEach
    public void teardown() {
        form.close();
    }

    private final TransformCdc<SinkRecord> form = new TransformCdc.Value<>();

    private void requireSet() {
        final Map<String, String> props = new HashMap<>();
        props.put("cdc", "cdc");
        form.configure(props);
    }

    private void result(Struct value, String print) {
        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = form.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        System.out.println(print + updatedValue);
    }

    @Test
    @DisplayName("init_test")
    void withSchemaInit() {
        requireSet();

        final Struct value = new Struct(initSchema);
        value.put("source", getInitSourceValue());
        value.put("databaseName", "ORCLPDB1");
        value.put("schemaName", "DEBEZIUM");

        result(value, "init updatedValue = ");
    }

    @Test
    @DisplayName("insert_test")
    void withSchemaInsert() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("after", getAfterValue("a", "a"));
        value.put("source", getSourceValue());
        value.put("op", "c");
        value.put("ts_ms", "1666770079594");

        result(value, "insert updatedValue = ");
    }

    @Test
    @DisplayName("delete_test")
    void withSchemaDelete() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("a", "a"));
        value.put("source", getSourceValue());
        value.put("op", "d");
        value.put("ts_ms", "1666770079594");

        result(value, "delete updatedValue = ");
    }

    @Test
    @DisplayName("update_test")
    void withSchemaUpdate() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("a", "a"));
        value.put("after", getAfterValue("a", "c"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update updatedValue = ");
    }

    /*
    @Test
    @DisplayName("maptest")
    void map (){
        Map<String, Object> map = new HashMap<>();

        String big = "big";
        String small = "small";

        map.put(small, "mall");

        Object pig = map.putIfAbsent(big, "pig"); //없으면 넣고 null 반환
        Object mall = map.computeIfAbsent(small, key -> "mall22222"); //없으면 넣고 두번째 파라미터로 계산된 것 반환, 있으면 가져옴

        System.out.println("map = " + map);
        System.out.println("mall = " + mall);
        System.out.println("pig = " + pig);
    }
    */
}

