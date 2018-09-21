package com.example.avrotest;

import java.io.*;

import com.example.avrotest.customlogicaltype.CustomDecimalConversion;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AvroTestUtil {

    public static GenericRecord toAvroRecord(String schemaFile, String jsonDataFile)
            throws IOException {
        GenericRecord avroData;
        org.apache.avro.Schema schema = parseAvroSchema(schemaFile);


        try (BufferedInputStream bufferedInputStream =
                     new BufferedInputStream(new FileInputStream(jsonDataFile))) {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema,
                    bufferedInputStream);
            DatumReader<GenericRecord> reader =
                    new GenericDatumReader<GenericRecord>(schema);
            avroData = reader.read(null, decoder);
        }
        return avroData;
    }

    public static String toAvroFile(String schemaFile, String jsonDataFile) throws Exception {
        String avroFileName = jsonDataFile.replaceAll(".json",".avro");
        OutputStream out = new FileOutputStream(avroFileName);
        DataFileWriter<GenericRecord> dataFileWriter = null;
        try {
            GenericRecord avroRecord = AvroTestUtil.toAvroRecord(schemaFile,
                    jsonDataFile);
            System.out.println(avroRecord.toString());

            DatumWriter<GenericRecord> datumWriter = new
                    GenericDatumWriter<GenericRecord>(parseAvroSchema(schemaFile));
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

            dataFileWriter.create(parseAvroSchema(schemaFile), out);

            dataFileWriter.append(avroRecord);

            dataFileWriter.flush();

            return avroFileName;
        } catch (Exception ex) {
            throw ex;
        } finally {
            out.close();
        }

    }

    private static Schema parseAvroSchema(String schemaFile) throws IOException {
        Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(new File(schemaFile));
    }


    public static List<GenericRecord> getAvroRecords(String avroSchemaFile, String avroFileName) throws Exception {
        Objects.requireNonNull(avroFileName,"Parameter 'avroFileName' can not be null.");

        GenericData GENERIC = GenericData.get();
        GENERIC.addLogicalTypeConversion(new CustomDecimalConversion());

        Schema schema = parseAvroSchema(avroSchemaFile);

        DatumReader<GenericRecord> datumReader = GENERIC.createDatumReader(schema);
        //DatumReader<GenericRecord> datumReader = new GenericDatumReader(schema);
        DataFileReader<GenericRecord> dataFileReader = null;

        List<GenericRecord> avroRecordList = new ArrayList<GenericRecord>();

        try {
            dataFileReader = new DataFileReader<GenericRecord>(new File(avroFileName), datumReader);
            GenericRecord avroRecord = null;
            while (dataFileReader.hasNext()) {
                avroRecord = dataFileReader.next(avroRecord);
                avroRecordList.add(avroRecord);
            }

        } catch (Exception ex){
            throw ex;
        } finally {
            dataFileReader.close();
        }
        return avroRecordList;

    }

    public static void printAvroFile(String avroSchemaFile, String avroFileName) throws Exception {
        List<GenericRecord> avroRecordList = getAvroRecords(avroSchemaFile, avroFileName);
        boolean printAvroSchema = true;
        Schema avroSchema = null;
        for (GenericRecord avroRecord : avroRecordList) {
            if( printAvroSchema) {
                avroSchema = avroRecord.getSchema();
                System.out.println("AVRO Schema = " + avroSchema);
                printAvroSchema = false;
            }
            printLogicalTypes(avroRecord);
            System.out.println(avroRecord.toString());
        }
    }

    public static void printLogicalTypes(GenericRecord avroRecord) {
        for (Schema.Field avroField : avroRecord.getSchema().getFields()) {
            Schema fieldSchema = avroField.schema();
            if( fieldSchema.getType() == Schema.Type.UNION ) {
                List<Schema> fieldTypes = fieldSchema.getTypes();
                for( Schema fSchema : fieldTypes) {
                    if( fSchema.getType() == Schema.Type.RECORD) {
                        printLogicalTypes((GenericRecord)avroRecord.get(fSchema.getName()));
                    } else if( fSchema != null) {
                        System.out.println("Field logical type = " + fSchema.getLogicalType() + " Field Schema = "  + fSchema + " type = " + fSchema.getType());
                    } else {
                        continue;
                    }
                }
            }
            if( fieldSchema.getType() == Schema.Type.RECORD) {
                printLogicalTypes((GenericRecord)avroRecord.get(fieldSchema.getName()));
            } else if( fieldSchema != null) {
                System.out.println("Field logical type = " + fieldSchema.getLogicalType() + " Field Schema = "  + fieldSchema + " type = " + fieldSchema.getType() + " value = " + avroRecord.get(avroField.name()) + " value class = " + avroRecord.get(avroField.name()).getClass());

            } else {
                continue;
            }
        }
    }
}
