package com.example.convertercdc;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Properties;

public abstract class ConverterCDC implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder metaSchema;

    @Override
    public void configure(Properties props) {
        metaSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        if ("op".equals(column.typeName())) {
            registration.register(metaSchema, Object::toString);
        }
    }
}
