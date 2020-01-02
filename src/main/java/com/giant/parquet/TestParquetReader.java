package com.giant.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

import java.io.IOException;

public class TestParquetReader {

    public static void main(String[] args) throws IOException {
        final String parquetFile = "output/parquet/data.parquet";
        ReadSupport<GenericRecord> readSupport = new AvroReadSupport();
        ParquetReader<GenericRecord> parquetReader = new ParquetReader<GenericRecord>(new Path(parquetFile), readSupport);
        GenericRecord record = parquetReader.read();
        System.out.println(record.get("id"));
        System.out.println(record.get("age"));
        System.out.println(record.get("name"));
        System.out.println(record.get("place"));
    }
}
