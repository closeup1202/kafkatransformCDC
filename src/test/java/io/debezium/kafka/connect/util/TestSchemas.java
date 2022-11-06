package io.debezium.kafka.connect.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public interface TestSchemas {

    Schema initSourceSchema = SchemaBuilder.struct()
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.STRING_SCHEMA)
            .field("snapshot", Schema.BOOLEAN_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("scn", Schema.STRING_SCHEMA)
            .field("ssn", Schema.STRING_SCHEMA)
            .build();

     Schema sourceSchema = SchemaBuilder.struct()
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("txId", Schema.STRING_SCHEMA)
            .field("scn", Schema.STRING_SCHEMA)
            .field("commit_scn", Schema.STRING_SCHEMA)
            .field("ssn", Schema.STRING_SCHEMA)
            .field("redo_thread", Schema.STRING_SCHEMA)
            .build();

     Schema transactionSchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();

     Schema beforeSchema = SchemaBuilder.struct()
            .field("ID", Schema.STRING_SCHEMA)
            .field("FIRST_NAME", Schema.STRING_SCHEMA)
            .field("LAST_NAME", Schema.STRING_SCHEMA)
            .field("EMAIL", Schema.STRING_SCHEMA)
            .build();

     Schema afterSchema = SchemaBuilder.struct()
            .field("ID", Schema.STRING_SCHEMA)
            .field("FIRST_NAME", Schema.STRING_SCHEMA)
            .field("LAST_NAME", Schema.STRING_SCHEMA)
            .field("EMAIL", Schema.STRING_SCHEMA)
            .build();

    /*-------------------------------------------------------------------------*/

    Schema initSchema = SchemaBuilder.struct()
            .field("source", initSourceSchema)
            .field("databaseName", Schema.STRING_SCHEMA)
            .field("schemaName", Schema.STRING_SCHEMA)
            //.field("ddl", Schema.STRING_SCHEMA)
            //.field("tableChanges", new SchemaBuilder(Schema.Type.ARRAY).build())
            .build();

     Schema schema = SchemaBuilder.struct()
            .field("before", beforeSchema)
            .field("after", afterSchema)
            .field("source", sourceSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.STRING_SCHEMA)
            .field("transaction", transactionSchema)
            .build();

    /*-------------------------------------------------------------------------*/

    Schema KDO_SCHEMA = SchemaBuilder.struct()
            .field("UNT_CLSF_CD", Schema.STRING_SCHEMA)
            .field("DRAFT_OULN_SEQNO", Schema.STRING_SCHEMA)
            .field("DRAFT_SUBJT", Schema.STRING_SCHEMA)
            .field("DRFTD_CRTN_DT", Schema.STRING_SCHEMA)
            .field("DRAFT_SNDNG_DEPT_NM", Schema.STRING_SCHEMA)
            .field("DRAFT_SNDNG_SYS_NM", Schema.STRING_SCHEMA)
            .field("FIRST_DRAFT_CRTN_DT", Schema.STRING_SCHEMA)
            .field("JOB_GB_CD", Schema.STRING_SCHEMA)
            .field("FIRST_DRAFT_OULN_SEQNO", Schema.STRING_SCHEMA)
            .field("DRAFT_PROCESS_STATE_CD", Schema.STRING_SCHEMA)
            .field("REPLY_PRRNG_DATE", Schema.STRING_SCHEMA)
            .field("DRFTD_SEQNO", Schema.STRING_SCHEMA)
            .field("FIRST_DRAFT_BDLT_TYPE1_CNTT", Schema.STRING_SCHEMA)
            .field("LAST_DRAFT_BDLT_TYPE1_CNTT", Schema.STRING_SCHEMA)
            .field("SIGUNGU_CD", Schema.STRING_SCHEMA)
            .field("FIRST_SIGUNGU_CD", Schema.STRING_SCHEMA)
            .field("AGDA_CNFR_GB_CD", Schema.STRING_SCHEMA)
            .field("PBSVC_GB_CD", Schema.STRING_SCHEMA)
            .field("REPLY_DRAFT_YN", Schema.STRING_SCHEMA)
            .field("FIRST_DRAFT_BDLT_TYPE2_CNTT", Schema.STRING_SCHEMA) //CLOB
            .field("LAST_DRAFT_BDLT_TYPE2_CNTT", Schema.STRING_SCHEMA) //CLOB
            .field("FIRST_CRTN_DT", Schema.STRING_SCHEMA)
            .field("LAST_UPDT_DT", Schema.STRING_SCHEMA)
            .build();

    Schema kdoDefaultSchema = SchemaBuilder.struct()
            .field("before", KDO_SCHEMA)
            .field("after", KDO_SCHEMA)
            .field("source", sourceSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.STRING_SCHEMA)
            .field("transaction", transactionSchema)
            .build();
}
