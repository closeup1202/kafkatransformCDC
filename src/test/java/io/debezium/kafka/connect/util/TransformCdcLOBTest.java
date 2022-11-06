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
        form.putTxIdForTesting();
    }

    private void result(Struct value, String print) {
        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = form.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        System.out.println(print + updatedValue);
    }

    @Test
    @DisplayName("update_lob_success : lob_piece O")
    void withSchemaUpdateLOB() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("a", "d"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_success2 : lob_piece O(2)")
    void withSchemaUpdateLOB2() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("[CLOB]", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_test_success updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail : lob_piece X")
    void withSchemaUpdateLOB_fail() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("[CLOB]", "c"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_fail updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null : [CLOB] - null :: lob_piece X")
    void withSchemaUpdateLOBNull() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("null", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_null updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null2 : [CLOB] - null (2이상) :: lob_piece X")
    void withSchemaUpdateLOBNull2() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_two_null updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_fail_null3 : [CLOB] - null (behind) :: lob_piece X")
    void withSchemaUpdateLOBNull3() {
        requireSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("[CLOB]", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "lob_two_null_back updatedValue = ");
    }

    @Test
    @DisplayName("update_lob_txId flag : txId(O), [CLOB] - a :: KEY O")
    void transactionValue_key (){
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("a", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "[CLOB] - a 이면 KEY를 삭제하지 않는다 = ");
    }

    @Test
    @DisplayName("update_lob_txId flag : txId(O), [CLOB] - null :: KEY X")
    void transactionValueCheck_deleteKey (){
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "a"));
        value.put("after", getAfterValue("null", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "txId(O) [CLOB] - null :::: KEY를 삭제 = ");
    }

    @Test
    @DisplayName("update_lob_txId flag : txId(O), [CLOB] - null :: KEY X")
    void transactionValueCheck_deleteKey2 (){
        txIdCacheSet();

        final Struct value = new Struct(schema);
        value.put("before", getBeforeValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterValue("null", "null"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "txId(O) [CLOB] - null :::: KEY를 삭제 = ");
    }

    @Test
    @DisplayName("KPP_DRAFT_OULN update lob(1) :: lob_piece X")
    void update_lob_kod_1 (){
        requireSet();

        final Struct value = new Struct(kdoDefaultSchema);
        value.put("before", getBeforeKDOValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterKDOValue("[CLOB]", "[CLOB]"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "KPP_DRAFT_OULN update lob(1) = ");
    }

    @Test
    @DisplayName("KPP_DRAFT_OULN update lob(2) :: lob_piece O")
    void update_lob_kod_2 (){
        requireSet();

        final Struct value = new Struct(kdoDefaultSchema);
        value.put("before", getBeforeKDOValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterKDOValue("a", "[CLOB]"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "KPP_DRAFT_OULN update lob(2) = ");
    }

    @Test
    @DisplayName("KPP_DRAFT_OULN update lob(3) :: lob_piece O")
    void update_lob_kod_3 (){
        requireSet();

        final Struct value = new Struct(kdoDefaultSchema);
        value.put("before", getBeforeKDOValue("[CLOB]", "[CLOB]"));
        value.put("after", getAfterKDOValue("[CLOB]", "a"));
        value.put("source", getSourceValue());
        value.put("op", "u");
        value.put("ts_ms", "1666770079594");

        result(value, "KPP_DRAFT_OULN update lob(3) = ");
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

