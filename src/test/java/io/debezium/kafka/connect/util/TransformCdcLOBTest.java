package io.debezium.kafka.connect.util;

import io.debezium.ConverterCdcApplicationTests;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.kafka.connect.util.TestSchemas.*;
import static io.debezium.kafka.connect.util.TestValues.*;

@SpringBootTest(classes = ConverterCdcApplicationTests.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TransformCdcLOBTest {

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

    private void txIdCacheSet() {
        requireSet();
        Map<String, Object> txIdCache = form.getTxIdCache();
        txIdCache.put("0a0014002f030000", "0a0014002f030000");
    }

    private void result(Struct value, String print) {
        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = form.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        System.out.println(print + updatedValue);
    }

    @Test
    @DisplayName("update_lob_no_tx : lob_piece x")
    void withSchemaUpdateLOBNoTX() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("a", "d"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_no_tx : lob_piece x updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_no_tx(2) : lob_piece x")
    void withSchemaUpdateLOBNoTx2() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_no_tx(2) : lob_piece x updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success : lob_piece O(1)")
    void withSchemaUpdateLOB() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("a", "d"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success : lob_piece O(2)")
    void withSchemaUpdateLOB2() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("[CLOB]", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success : lob_piece O(3)")
    void withSchemaUpdateLOB3() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success : lob_piece O(4)")
    void withSchemaUpdateLOB4() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("a", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success [CLOB]2 : a, [CLOB] :: lob_piece O")
    void withSchemaUpdateLOB5(){
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("a", "[CLOB]"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_success [CLOB] [CLOB] : a, [CLOB] = ");
    }

    @Test
    @DisplayName("update_lob_no event")
    void noEvent() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob no event = ");
    }

    @Test
    @DisplayName("update_lob_ok event")
    void okEvent() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob ok event = ");
    }

    @Test
    @DisplayName("update_lob_fail : [CLOB] - a : [CLOB] - c :: lob_piece X")
    void withSchemaUpdateLOB_fail() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("[CLOB]", "c"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_fail : [CLOB] - a : [CLOB] - c updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null : [CLOB] - 'a' : null - 'a' :: lob_piece X")
    void withSchemaUpdateLOBNull() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("null", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_fail_null : [CLOB] - 'a' : null - 'a' updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null2 : [CLOB] - [CLOB] : null - null  :: lob_piece X")
    void withSchemaUpdateLOBNull2() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_fail_null2 : [CLOB] - [CLOB] : null - null updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null3 : [CLOB] - [CLOB] : [CLOB] - null :: lob_piece X")
    void withSchemaUpdateLOBNull3() {
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("[CLOB]", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_fail_null3 : [CLOB] - [CLOB] : [CLOB] - null updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_[CLOB]2 : [CLOB]2 :: lob_piece X")
    void update_lob_kod_1 (){
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("[CLOB]", "[CLOB]"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "update_lob_fail_[CLOB]2 : [CLOB]2 = ");
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

