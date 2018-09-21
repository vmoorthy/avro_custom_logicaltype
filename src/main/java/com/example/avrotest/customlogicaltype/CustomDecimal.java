package com.example.avrotest.customlogicaltype;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;


public class CustomDecimal extends LogicalType {
    private static final String PRECISION_PROP = "precision";
    private static final String SCALE_PROP = "scale";
    private final int precision;
    private final int scale;

    public CustomDecimal(int precision, int scale) {
        super("custom_decimal");
        this.precision = precision;
        this.scale = scale;
    }

    public CustomDecimal(Schema schema) {
        super("custom_decimal");
        if (!this.hasProperty(schema, "precision")) {
            throw new IllegalArgumentException("Invalid decimal: missing precision");
        } else {
            this.precision = this.getInt(schema, "precision");
            if (this.hasProperty(schema, "scale")) {
                this.scale = this.getInt(schema, "scale");
            } else {
                this.scale = 0;
            }

        }
    }

    public Schema addToSchema(Schema schema) {
        super.addToSchema(schema);
        schema.addProp("precision", this.precision);
        schema.addProp("scale", this.scale);
        return schema;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.STRING ) {
            throw new IllegalArgumentException("Logical type decimal must be backed by string");
        } else if (this.precision <= 0) {
            throw new IllegalArgumentException("Invalid decimal precision: " + this.precision + " (must be positive)");
        } else if ((long)this.precision > this.maxPrecision(schema)) {
            throw new IllegalArgumentException("string(" + schema.getFixedSize() + ") cannot store " + this.precision + " digits (max " + this.maxPrecision(schema) + ")");
        } else if (this.scale < 0) {
            throw new IllegalArgumentException("Invalid decimal scale: " + this.scale + " (must be positive)");
        } else if (this.scale > this.precision) {
            throw new IllegalArgumentException("Invalid decimal scale: " + this.scale + " (greater than precision: " + this.precision + ")");
        }
    }

    private long maxPrecision(Schema schema) {
        if (schema.getType() == Schema.Type.STRING) {
            return 2147483647L;
        } else {
            return 0L;
        }
    }

    private boolean hasProperty(Schema schema, String name) {
        return schema.getObjectProp(name) != null;
    }

    private int getInt(Schema schema, String name) {
        Object obj = schema.getObjectProp(name);
        if (obj instanceof Integer) {
            return (Integer)obj;
        } else {
            throw new IllegalArgumentException("Expected int " + name + ": " + (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            CustomDecimal decimal = (CustomDecimal)o;
            if (this.precision != decimal.precision) {
                return false;
            } else {
                return this.scale == decimal.scale;
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = this.precision;
        result = 31 * result + this.scale;
        return result;
    }
}