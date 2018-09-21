package com.example.avrotest.customlogicaltype;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class CustomDecimalLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {

    @Override
    public LogicalType fromSchema(Schema schema) {
        return new CustomDecimal(schema);
    }
}

