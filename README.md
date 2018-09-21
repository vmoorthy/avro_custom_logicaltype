# avro_custom_logicaltype
This repository has an example of writing avro custom logical type in java.

To create any new custom logical type, here are the steps to follow:
1) Create a custom logical type class which should extend org.apache.avro.LogicalType class. 
Refer com.example.avrotest.customlogicaltype.CustomDecimal class.
2) Create a custom conversion class which should extend org.apache.avro.Conversion class. 
Refer org.apache.avro.CustomDecimalConversion
3) Create a LogicalTypeFactory class which should implement LogicalTypes.LogicalTypeFactory. 
Refer com.example.avrotest.customlogicaltype.CustomDecimalLogicalTypeFactory
4) Register your custom logical type factory class.
Ex:- <br/>
      LogicalTypes.register("custom_decimal",new CustomDecimalLogicalTypeFactory());
      
5) Register your custom conversion class with GenericData so that DatumReader or DatumWriter can do the appropriate conversion.
Ex:- <br/>
      GenericData GENERIC = GenericData.get();
      GENERIC.addLogicalTypeConversion(new CustomDecimalConversion());


