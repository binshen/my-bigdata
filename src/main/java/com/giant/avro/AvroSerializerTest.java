package com.giant.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroSerializerTest {

    public static void main(String[] args) throws IOException {

        String avscFilePath = "config/user.avsc";
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Tony");
        user1.put("number", 18);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("number", 3);
        user2.put("color", "red");

        File file = new File("output/avro/result.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }
}
