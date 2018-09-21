package com.example.avrotest;

import com.example.avrotest.customlogicaltype.CustomDecimalLogicalTypeFactory;
import org.apache.avro.LogicalTypes;

public class Main {

    public static void main(String[] args)  throws Exception, Throwable {

        LogicalTypes.register("custom_decimal",new CustomDecimalLogicalTypeFactory());

        String avroFileName = AvroTestUtil.toAvroFile("src/main/resources/test.avsc","src/main/resources/test.json");

        AvroTestUtil.printAvroFile("src/main/resources/test.avsc", avroFileName);

    }
}
