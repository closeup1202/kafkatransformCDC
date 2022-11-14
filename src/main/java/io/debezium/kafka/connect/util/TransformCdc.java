package io.debezium.kafka.connect.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class TransformCdc<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String CDC_VALUE = "cdc";
    private static final String PURPOSE = "Transform into CDC form";
    private static final String CLOB = "[CLOB]";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CDC_VALUE, ConfigDef.Type.STRING, "cdc", ConfigDef.Importance.HIGH, PURPOSE);

    private String cdc;
    private Map<String, String> renameMap;
    private Map<String, String> reverseRenameMap;
    private ObjectMapper objectMapper;
    private Map<String, Object> txIdCache;
    private List<String> txIdList;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        cdc = config.getString(CDC_VALUE);
        renameMap = Map.of(Fields.BEFORE, Fields.KEY, Fields.AFTER, Fields.DATA, Fields.SOURCE, Fields.META);
        objectMapper = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        reverseRenameMap = reverse(renameMap);
        txIdCache = new LinkedHashMap<>();
        txIdList = new ArrayList<>();
    }

    /* 테스트용 */
    public Map<String, Object> getTxIdCache(){
        return this.txIdCache;
    }

    private Map<String, String> reverse(Map<String, String> source) {
        final Map<String, String> reverseMap = new ConcurrentHashMap<>();
        source.forEach((key, value) -> reverseMap.put(value, key));
        return reverseMap;
    }

    @Override
    public R apply(R record) {
        if(Objects.isNull(operatingSchema(record))){
            return record;
        } else {
            txIdList.clear();
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);
        final Schema updatedSchema = makeUpdatedSchema(value);
        Struct updatedValue = new Struct(updatedSchema);

        Field op = value.schema().field(Fields.OP);

        for (Field field : updatedSchema.fields()) {
            updatedValue.put(field.name(), getValue(value, op, field));
        }

        try {
            return cutDownRecord(record, updatedSchema, updatedValue);
        } catch (DataException e){
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Object getValue(Struct value, Field op, Field field) {
        return (Objects.nonNull(op) && Objects.equals(field.name(), Fields.META))
                ? convertSourceToMeta(value, field.name(), op)
                : value.get(reverseRenamed(field.name()));
    }

    private Schema makeUpdatedSchema(Struct value) {
        final Schema schema = value.schema();
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        final Field op = schema.field(Fields.OP);

        if(Objects.isNull(op)){
            return dmlSchema(schema, builder);
        }

        schema.fields().stream()
                       .filter (field -> Objects.equals(field.name(), Fields.SOURCE))
                       .forEach(field -> builder.field(renamed(field.name()), metaSchema(value)));

        schema.fields().stream()
                       .filter (field -> filter(field.name(), value))
                       .forEach(field -> builder.field(renamed(field.name()), field.schema()));

        return builder.build();
    }

    private Schema dmlSchema(Schema schema, SchemaBuilder builder) {
        schema.fields().forEach(field -> builder.field(field.name(), field.schema()));
        return builder.build();
    }

    private String reverseRenamed(String name) {
        return reverseRenameMap.getOrDefault(name, name);
    }

    private Schema metaSchema(Struct value) {
        return hasLobPiece(value) ? MetaSchema.LOB : MetaSchema.BASIC;
    }

    private Struct convertSourceToMeta(Struct value, String fieldName, Field op) {
        final Struct source = (Struct) value.get(reverseRenamed(fieldName));
        final String opValue = value.get(op).toString().toUpperCase();

        if(hasLobPiece(value)){
            final Struct struct = metaStruct(source, opValue, MetaSchema.LOB);
            struct.put(MetaSchema.Fields.LOB_PIECE, "last");
            return struct;
        }

        return metaStruct(source, opValue, MetaSchema.BASIC);
    }

    private boolean hasLobPiece(Struct value){
        final Object[] objects = keyAndData(value);
        final Struct source = (Struct) value.get(Fields.SOURCE);
        final Object txId = source.get("txId");

        if(Objects.isNull(txId) || ArrayUtils.contains(objects, null)){
            return false;
        }

        if(Objects.isNull(txIdCache.get(txId.toString()))){
            System.out.println("hasLobPiece :: txId null");
            return false;
        }

        Object key = objects[0];
        Object data = objects[1];

        return key.toString().contains(CLOB) && checkClobCounterIndex(key, data);
    }

    private Struct metaStruct(Struct source, String opValue, Schema schema){
        final Struct struct = new Struct(schema);
        final Object txId = source.get("txId");

        struct.put(MetaSchema.Fields.TABLE,      source.get("table"));
        struct.put(MetaSchema.Fields.CSCN,       source.get("scn"));
        struct.put(MetaSchema.Fields.TXID,       Objects.isNull(txId) ? null : txIdCache.computeIfAbsent(txId.toString(), key -> {txIdList.add(txId.toString()); return txId;}));
        struct.put(MetaSchema.Fields.OP,         Objects.equals(opValue, "C") ? "I" : opValue);
        struct.put(MetaSchema.Fields.CURRENT_TS, DateFormatCdc.convert(source.get("ts_ms")));

        return struct;
    }

    private R cutDownRecord(R record, Schema schema, Struct value) {
        List<Integer> integers = getDiffIndexToList(value);
        Struct dataStruct = (Struct) value.get(Fields.DATA);
        List<Field> dataList = dataStruct.schema().fields();

        Schema childSchema = ChangedDataSchema.getSchema(integers, dataList);
        Struct childValue = new Struct(childSchema);
        integers.forEach(i -> childValue.put(dataList.get(i).name(), dataStruct.get(dataList.get(i))));

        final Schema parentSchema = ChangedDataSchema.parentSchema(schema, childValue);
        final Struct parentValue = ChangedDataSchema.parentValue(value, childValue, parentSchema);

        if(inCaseLobNullInsert(value, parentValue)){
            System.out.println("unnecessary....");
            return unnecessaryRecord(record);
        }

        return newRecord(record, parentSchema, parentValue);
    }

    private boolean inCaseLobNullInsert(Struct value, Struct parentValue) {
        Struct meta = (Struct) value.get(Fields.META);
        Object key = value.get(Fields.KEY);
        Object data = parentValue.get(Fields.DATA);
        List<String> keyList = convertValuesToList(key);
        List<String> dataValueList = convertValuesToList(data);
        String op = meta.get(Fields.OP).toString();
        String txId = meta.get(MetaSchema.Fields.TXID).toString();

        long keyClobCount = getCount(keyList, CLOB);
        long dataNullCount = getCount(dataValueList, "null");

        return keyClobCount == dataNullCount && Objects.equals(op, "U") && !txIdList.contains(txId);
    }

    private long getCount(List<String> list, String include) {
        return list.stream().filter(item -> Objects.equals(item, include)).count();
    }

    private List<Integer> getDiffIndexToList(Struct value) {
        Object key = value.get(Fields.KEY);
        Object data = value.get(Fields.DATA);
        List<String> keyValueList = convertValuesToList(key);
        List<String> dataValueList = convertValuesToList(data);

        if(Objects.equals(key, data)){
            throw new DataException("unchanged...");
        }

        List<Integer> list = new ArrayList<>();

        IntStream.range(0, keyValueList.size())
                 .filter(i -> !Objects.equals(keyValueList.get(i), dataValueList.get(i)))
                 .forEach(list::add);

        return list;
    }

    /* [CLOB] 과 대응되는 컬럼 체크하여 lob_piece 부착 여부 결정 */
    private boolean checkClobCounterIndex(Object key, Object data) {
        List<String> keyList = convertValuesToList(key);
        List<String> dateList = convertValuesToList(data);

        if(!keyList.contains(CLOB)){
            return false;
        }

        return countMismatch(keyList, dateList) && isNullClobColumn(keyList, dateList);
    }

    /* key - data = [CLOB] : [CLOB] 개수 비교 */
    private boolean countMismatch(List<String> key, List<String> data) {
        long keyClobCount = getCount(key, CLOB);
        long dataClobCount = getCount(data, CLOB);
        return keyClobCount != dataClobCount;
    }

    /* key - data = [CLOB] : null 비교 */
    private boolean isNullClobColumn(List<String> key, List<String> data) {
        List<Integer> clobColumns = new ArrayList<>();

        IntStream.range(0, key.size())
                 .filter(i -> Objects.equals(key.get(i), CLOB))
                 .forEach(clobColumns::add);

        for (Integer i : clobColumns) {
            if(!Objects.equals(data.get(i), "null") && !Objects.equals(data.get(i), CLOB)){
                return true;
            }
        }
        return false;
    }

    private List<String> convertValuesToList(Object struct) {
        Map<String, Object> map = objectMapper.convertValue(struct, Map.class);
        final Object values = map.get("values");
        final String[] split = StringUtilsCdc.subStringAndSplit(values.toString());
        return Arrays.stream(split).collect(Collectors.toList());
    }

    private boolean filter(String fieldName, Struct value) {
        if(Fields.EXCLUDE.contains(fieldName)){
            return false;
        }

        final String op = value.get(Fields.OP).toString();

        return operationFilter(fieldName, op);
    }

    private boolean operationFilter(String fieldName, String op) {
        switch (op){
            case Operation.INSERT:
                return !Objects.equals(fieldName, Fields.BEFORE);

            case Operation.DELETE:
                return !Objects.equals(fieldName, Fields.AFTER);

            default:
                return true;
        }
    }

    private Object[] keyAndData(Struct value){
        Object key = value.get(Fields.BEFORE);
        Object data = value.get(Fields.AFTER);
        return new Object[]{key, data};
    }

    private String renamed(String fieldName) {
        return renameMap.getOrDefault(fieldName, fieldName);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        txIdCache.clear();
        txIdList.clear();
    }

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);
    protected abstract R unnecessaryRecord(R record);

    public static class Key<R extends ConnectRecord<R>> extends TransformCdc<R> {

        @Override
        protected Schema operatingSchema(R record) { return record.keySchema(); }

        @Override
        protected Object operatingValue(R record) { return record.key(); }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected R unnecessaryRecord(R record) {
            return record.newRecord("garbage-collect-debezium", 0, null, null, null, null, null);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TransformCdc<R> {

        @Override
        protected Schema operatingSchema(R record) { return record.valueSchema(); }

        @Override
        protected Object operatingValue(R record) { return record.value(); }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

        @Override
        protected R unnecessaryRecord(R record) {
            return record.newRecord("garbage-collect-debezium", 0, null, null, null, null, null);
        }
    }
}
