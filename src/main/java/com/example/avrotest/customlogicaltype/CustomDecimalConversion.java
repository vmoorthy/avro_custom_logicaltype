package com.example.avrotest.customlogicaltype;

import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

import java.math.BigDecimal;


public class CustomDecimalConversion extends Conversion<BigDecimal> {
    public CustomDecimalConversion() {
    }

    public Class<BigDecimal> getConvertedType() {
        return BigDecimal.class;
    }

    public Schema getRecommendedSchema() {
        throw new UnsupportedOperationException("No recommended schema for custom_decimal (scale is required)");
    }

    public String getLogicalTypeName() {
        return "custom_decimal";
    }

    public BigDecimal fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        int scale = ((CustomDecimal)type).getScale();
        return new BigDecimal(value.toString()).setScale(scale);
    }

    public CharSequence toCharSequence(BigDecimal value, Schema schema, LogicalType type) {
        int scale = ((CustomDecimal)type).getScale();
        if (scale != value.scale()) {
            throw new AvroTypeException("Cannot encode custom_decimal with scale " + value.scale() + " as scale " + scale);
        } else {
            return value.toString();
        }
    }
}