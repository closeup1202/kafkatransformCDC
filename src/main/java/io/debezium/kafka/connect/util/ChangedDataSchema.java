package io.debezium.kafka.connect.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.List;
import java.util.Objects;

public class ChangedDataSchema {

    private ChangedDataSchema() throws Exception {
        throw new UtilClassException();
    }

    public static Schema getSchema(List<Integer> integers, List<Field> dataList) {
        SchemaBuilder newDataSourceSchema = SchemaUtil.copySchemaBasics(SchemaBuilder.struct());
        integers.forEach(i -> newDataSourceSchema.field(dataList.get(i).name(), dataList.get(i).schema()));
        return newDataSourceSchema.build();
    }

    public static Schema parentSchema(Schema schema, Struct childValue) {
        SchemaBuilder scm = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        schema.fields().forEach(field -> scm.field(field.name(), isEquals(field) ? childValue.schema() : field.schema()));
        return scm.build();
    }

    public static Struct parentValue(Struct value, Struct childValue, Schema parentSchema) {
        final Struct newUpdatedValue = new Struct(parentSchema);
        parentSchema.fields().forEach(field -> newUpdatedValue.put(field.name(), isEquals(field) ? childValue : value.get(field.name())));
        return newUpdatedValue;
    }

    private static boolean isEquals(Field field) {
        return Objects.equals(field.name(), Fields.DATA);
    }


}
