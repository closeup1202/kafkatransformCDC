package io.debezium.kafka.connect.util;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

public abstract class TransformCdc<R extends ConnectRecord<R>> implements Transformation<R> {

    //before, after, op
    private static final String CDC_VALUE = "cdc";
    private static final String PURPOSE = "transforming like cdc";

    interface Operation{
        String INSERT = "c";
        String DELETE = "d";
        String UPDATE = "u";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CDC_VALUE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, "Field name for transforming into cdc");

    private Cache<Schema, Schema> schemaUpdateCache;
    private List<String> fieldList;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldList = config.getList(CDC_VALUE);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        return (Objects.isNull(operatingSchema(record))) ? record : applyWithSchema(record);
    }

    private R applyWithSchema(R record) {
        final Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(value.schema()); // 캐시에서 벨류 스키마 가져옴 before, after, op ....
        if(Objects.isNull(updatedSchema)) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        // 스키마에 존재하는 것만 op의 따라 필터가 변함 (제외 리스트가 변함)
        for (Field field : value.schema().fields()) {
            Field op = updatedValue.schema().field("op");
            if(filter(field, op.schema())){
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    boolean filter(Field field, Schema opValue) {
        String name = opValue.name();
        if (Operation.INSERT.equals(name)) {
            return !field.name().equals("before");
        } else if (Operation.DELETE.equals(name)) {
            return !field.name().equals("after");
        } else if (Operation.UPDATE.equals(name)) {
            return true;
        }
        throw new ConfigException(opValue + "is not supported type");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends TransformCdc<R> {

        @Override
        protected Schema operatingSchema(R record) { return record.keySchema(); }

        @Override
        protected Object operatingValue(R record) { return record.key(); }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
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

    }

}
