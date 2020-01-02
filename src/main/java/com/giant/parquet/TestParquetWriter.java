package com.giant.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;

public class TestParquetWriter {

    public static void main(String[] args) throws IOException {
        final String schemaLocation = "input/avro_format.json";
        final Schema avroSchema = new Schema.Parser().parse(new File(schemaLocation));
        final MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        final WriteSupport<Pojo> writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
        final String parquetFile = "output/parquet/data.parquet";
        final Path path = new Path(parquetFile);
        ParquetWriter<GenericRecord> parquetWriter = new ParquetWriter(path, writeSupport);
        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", 1);
        record.put("age", 10);
        record.put("name", "ABC");
        record.put("place", "BCD");
        parquetWriter.write(record);
        parquetWriter.close();
    }
}
