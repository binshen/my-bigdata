package com.giant.convertor;

import com.giant.avro.schema.Destination;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Class that converts SVN to AVRO
 * @param <T> T type of wrapper.
 */
public class ParquetDataWorker<T> {

    private static final Logger logger = LogManager.getLogger(ParquetDataWorker.class.getName());

    public static void main(String[] args) throws Exception {
        csvToParquet("input/csv/destinations.csv", "output/convertor/parquet_data", Destination.getClassSchema());
    }

    /**
     *
     * @param input pqrquet file
     * @param <T> Generic type
     * @return List of Generic results
     * @throws Exception
     */
    public static  <T> List<T> parquetToObj(File input) throws Exception {

        logger.log(Level.INFO, "reading parquet file");

        ParquetReader<T> reader = null;
        Path path = new Path(input.getAbsolutePath());
        List<T> result = new ArrayList<>();
        try {
            reader = AvroParquetReader
                    .<T>builder(path)
                    .withConf(new Configuration())
                    .build();
            T record;
            while ((record = reader.read()) != null) {
                result.add(record);
            }
        }catch (Exception e){
                logger.log(Level.ERROR, "error",e );
                throw new Exception("can't convert parquet to obj", e);
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.log(Level.ERROR, "error", e);
                }
            }
        }
        return result;
    }

    /**
     * CVN to Parquet
     * @param inputPath input path
     * @param outputPath out path
     * @param schema schema of wrapper
     * @param <T> Generic type
     * @throws Exception if process fails
     */
    public static <T extends SpecificRecordBase> void csvToParquet(String inputPath, String outputPath, Schema schema) throws Exception {

        logger.log(Level.INFO, "csv to parquet");

        Path path = new Path(outputPath);
        ParquetWriter<GenericData.Record> writer = null;
        File theFile = new File(inputPath);
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(theFile, "UTF-8");
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();

            String header = it.nextLine();
            String[] headerComp = header.split(",");
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] lineComponents = line.split(",");
                GenericData.Record record = new GenericData.Record(schema);
                for (int i = 0; i < lineComponents.length; i++) {

                    if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.INT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            record.put(headerComp[i], 0);
                        }else{
                            record.put(headerComp[i], Integer.parseInt(lineComponents[i]));
                        }
                    } else if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.FLOAT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            record.put(headerComp[i], 0.0f);
                        }else{
                            record.put(headerComp[i], Float.parseFloat(lineComponents[i]));
                        }
                    } else {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            record.put(headerComp[i], "");
                        }else{
                            record.put(headerComp[i], lineComponents[i]);
                        }
                    }
                }
                writer.write(record);
            }
        } catch (Exception e) {
            logger.log(Level.ERROR, "can't convert csv to parquet", e);
            throw new Exception("can't convert csv to parquet", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.log(Level.ERROR, "error", e);
                }
            }
            it.close();
        }
    }
}
