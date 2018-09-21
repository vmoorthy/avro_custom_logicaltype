package com.example.avrotest;

import org.apache.avro.LogicalTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Main {

    public static void main(String[] args)  throws Exception, Throwable {

        LogicalTypes.register("custom_decimal",new CustomDecimalLogicalTypeFactory());

        String avroFileName = AvroTestUtil.toAvroFile("src/main/resources/test.avsc","src/main/resources/test.json");

        AvroTestUtil.printAvroFile("src/main/resources/test.avsc", avroFileName);

    }
}
