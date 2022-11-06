package io.debezium.kafka.connect.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public final class MetaSchema {

    private MetaSchema() throws Exception {
        throw new UtilClassException();
    }

    static final Schema BASIC = SchemaBuilder.struct()
            .field(Fields.TABLE,      Schema.STRING_SCHEMA)
            .field(Fields.CSCN,       Schema.STRING_SCHEMA)
            .field(Fields.TXID,       Schema.OPTIONAL_STRING_SCHEMA)
            .field(Fields.OP,         Schema.STRING_SCHEMA)
            .field(Fields.CURRENT_TS, Schema.STRING_SCHEMA)
            .build();

    static final Schema LOB = SchemaBuilder.struct()
            .field(Fields.TABLE,      Schema.STRING_SCHEMA)
            .field(Fields.CSCN,       Schema.STRING_SCHEMA)
            .field(Fields.TXID,       Schema.OPTIONAL_STRING_SCHEMA)
            .field(Fields.OP,         Schema.STRING_SCHEMA)
            .field(Fields.CURRENT_TS, Schema.STRING_SCHEMA)
            .field(Fields.LOB_PIECE,  Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private Schema addField(Schema schema, String fieldName, Schema schemaParam){
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(SchemaBuilder.struct());
        schema.fields().forEach(field -> builder.field(field.name(), field.schema()));
        builder.field(fieldName,  schemaParam);
        return builder.build();
    }

    static final class Fields{

        private Fields(){ }

        static final String TABLE = "table";
        static final String CSCN = "cscn";
        static final String TXID = "txid";
        static final String OP = "op";
        static final String CURRENT_TS = "current_ts";
        static final String LOB_PIECE = "lob_piece";
    }

}
