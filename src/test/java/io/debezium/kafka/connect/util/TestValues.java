package io.debezium.kafka.connect.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import static io.debezium.kafka.connect.util.TestSchemas.*;

public interface TestValues {

    static Struct getBeforeValue(String col1, String col2){
        final Struct beforeValue = new Struct(beforeSchema);
        beforeValue.put("ID", "1101");
        beforeValue.put("FIRST_NAME", col1);
        beforeValue.put("LAST_NAME", "a");
        beforeValue.put("EMAIL", col2);
        return beforeValue;
    }

    static Struct getAfterValue(String col1, String col2){
        final Struct afterValue = new Struct(afterSchema);
        afterValue.put("ID", "1101");
        afterValue.put("FIRST_NAME", col1);
        afterValue.put("LAST_NAME", "a");
        afterValue.put("EMAIL", col2);

        return afterValue;
    }

    static Struct getSourceValue(){
        final Struct sourceValue = new Struct(sourceSchema);
        sourceValue.put("version", "1.9.6.Final");
        sourceValue.put("connector", "oracle");
        sourceValue.put("name", "gmdcdctest2");
        sourceValue.put("ts_ms", "1666770046000");
        sourceValue.put("db", "ORCLPDB1");
        sourceValue.put("schema", "DEBEZIUM");
        sourceValue.put("table", "CUSTOMERS");
        sourceValue.put("txId", "0a0014002f030000");
        sourceValue.put("scn", "2634359");
        sourceValue.put("commit_scn", "2634406");
        sourceValue.put("ssn", "0");
        sourceValue.put("redo_thread","1");

        return sourceValue;
    }

    static Struct getInitSourceValue(){
        final Struct sourceValue = new Struct(initSourceSchema);
        sourceValue.put("version", "1.9.6.Final");
        sourceValue.put("connector", "oracle");
        sourceValue.put("name", "gmdcdctest2");
        sourceValue.put("ts_ms", "1666770046000");
        sourceValue.put("snapshot", true);
        sourceValue.put("db", "ORCLPDB1");
        sourceValue.put("schema", "DEBEZIUM");
        sourceValue.put("table", "CUSTOMERS");
        sourceValue.put("scn", "2634359");
        sourceValue.put("ssn", "0");

        return sourceValue;
    }

    // -------------------------------------------------------------- //

    static Struct getBeforeKDOValue(String col1, String col2){
        final Struct value = new Struct(KDO_SCHEMA);
        value.put("UNT_CLSF_CD", "a");
        value.put("DRAFT_OULN_SEQNO", "1");
        value.put("DRAFT_SUBJT", "a");
        value.put("DRFTD_CRTN_DT", "a");
        value.put("DRAFT_SNDNG_DEPT_NM", "a");
        value.put("DRAFT_SNDNG_SYS_NM", "a");
        value.put("FIRST_DRAFT_CRTN_DT", "a");
        value.put("JOB_GB_CD", "a");
        value.put("FIRST_DRAFT_OULN_SEQNO", "1");
        value.put("DRAFT_PROCESS_STATE_CD", "a");
        value.put("REPLY_PRRNG_DATE", "a");
        value.put("DRFTD_SEQNO", "1");
        value.put("FIRST_DRAFT_BDLT_TYPE1_CNTT", "a");
        value.put("LAST_DRAFT_BDLT_TYPE1_CNTT", "a");
        value.put("SIGUNGU_CD", "a");
        value.put("FIRST_SIGUNGU_CD", "a");
        value.put("AGDA_CNFR_GB_CD", "a");
        value.put("PBSVC_GB_CD", "a");
        value.put("REPLY_DRAFT_YN", "a");
        value.put("FIRST_DRAFT_BDLT_TYPE2_CNTT", col1); //CLOB
        value.put("LAST_DRAFT_BDLT_TYPE2_CNTT", col2); //CLOB
        value.put("FIRST_CRTN_DT", "a");
        value.put("LAST_UPDT_DT", "a");
        return value;
    }

    static Struct getAfterKDOValue(String col1, String col2){
        final Struct value = new Struct(KDO_SCHEMA);
        value.put("UNT_CLSF_CD", "a");
        value.put("DRAFT_OULN_SEQNO", "1");
        value.put("DRAFT_SUBJT", "a");
        value.put("DRFTD_CRTN_DT", "a");
        value.put("DRAFT_SNDNG_DEPT_NM", "a");
        value.put("DRAFT_SNDNG_SYS_NM", "a");
        value.put("FIRST_DRAFT_CRTN_DT", "a");
        value.put("JOB_GB_CD", "a");
        value.put("FIRST_DRAFT_OULN_SEQNO", "1");
        value.put("DRAFT_PROCESS_STATE_CD", "a");
        value.put("REPLY_PRRNG_DATE", "a");
        value.put("DRFTD_SEQNO", "1");
        value.put("FIRST_DRAFT_BDLT_TYPE1_CNTT", "a");
        value.put("LAST_DRAFT_BDLT_TYPE1_CNTT", "a");
        value.put("SIGUNGU_CD", "a");
        value.put("FIRST_SIGUNGU_CD", "a");
        value.put("AGDA_CNFR_GB_CD", "a");
        value.put("PBSVC_GB_CD", "a");
        value.put("REPLY_DRAFT_YN", "a");
        value.put("FIRST_DRAFT_BDLT_TYPE2_CNTT", col1); //CLOB
        value.put("LAST_DRAFT_BDLT_TYPE2_CNTT", col2); //CLOB
        value.put("FIRST_CRTN_DT", "a");
        value.put("LAST_UPDT_DT", "a");
        return value;
    }
}
