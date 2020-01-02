package com.giant.convertor;

import com.giant.avro.schema.Destination;
import com.google.common.collect.Streams;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class that converts SVN to AVRO
 */
public class AvroDataWorker {

    private static final Logger logger = LogManager.getLogger(AvroDataWorker.class.getName());

    public static void main(String[] args) throws Exception {

        csvToAvro("input/csv/destinations.csv", "output/convertor/avro_data", Destination.getClassSchema(), Destination.class);
    }

    /**
     * Converts SVN to Avro.
     * @param inputPath input path of document
     * @param outputPath output path of the document
     * @param schema schema of the document
     * @param clazz class of the concrete wrapper object
     * @param <T> Generic Record Type
     * @throws Exception if process is failed
     */
    public static <T extends SpecificRecordBase> void csvToAvro(String inputPath, String outputPath, Schema schema, Class<T> clazz) throws Exception {

        logger.log( Level.INFO, "csv to schema converting started");
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(clazz);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        LineIterator it = null;

        try {
            dataFileWriter.create(schema, new File(outputPath));
            it = FileUtils.lineIterator(new File(inputPath), "UTF-8");
            String header = it.nextLine();
            String[] headerComp = header.split(",");
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] lineComponents = line.split(",");
                T dest = clazz.newInstance();
                for (int i = 0; i < lineComponents.length; i++) {
                    if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.INT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, 0);
                        }else{
                            dest.put(i, Integer.parseInt(lineComponents[i]));
                        }
                    } else if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.FLOAT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, 0.0f);
                        }else{
                            dest.put(i, Float.parseFloat(lineComponents[i]));
                        }
                    } else {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, "");
                        }else{
                            dest.put(i, lineComponents[i]);
                        }
                    }
                }
                dataFileWriter.append(dest);
            }
        }catch (Exception e){
            logger.error("can't convert csv to avro", e);
            throw new Exception("can't convert json to schema record", e);
        }
        finally {
            try {
                it.close();
                dataFileWriter.close();
            } catch (IOException e) {
                logger.error("can't close reader/writers ", e);
            }
        }
    }

    /**
     * Conevrts avro to Java object
     * @param input avro file
     * @param clazz class of object wrapper
     * @param <T> Generic type
     * @return list of Records
     * @throws Exception if process failed
     */
    public static <T extends SpecificRecordBase> List<T> avroToObj(File input, Class<T> clazz) throws Exception {
        try {
            DatumReader<T> userDatumReader = new SpecificDatumReader<>(clazz);
            DataFileReader<T> dataFileReader = new DataFileReader<>(input, userDatumReader);
            return Streams.stream(dataFileReader.iterator()).collect(Collectors.toList());
        }catch (Exception e){
            logger.error("can't convert avro to obj", e);
            throw new Exception("can't convert avro to obj", e);
        }

    }

    /**
     * Converts JSON to Generic object
     *
     * @param json   json in String
     * @param schema Schema object
     * @return Generic record
     * @throws Exception if we cant parse json and can't convert to Generic record
     */
    public static GenericRecord fromJsonToAvro(String json, Schema schema) throws Exception {

        try {
            InputStream input = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(input);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
            Object datum = reader.read(null, decoder);

            GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            w.write(datum, encoder);
            encoder.flush();

            DatumReader<GenericRecord> genericRecordDatumReader = new GenericDatumReader<>(schema);
            Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
            logger.log(Level.INFO, "converted from json to schema");
            return genericRecordDatumReader.read(null, binaryDecoder);
        } catch (Exception e) {
            logger.error("can't convert json to schema record", e);
            throw new Exception("can't convert json to schema record", e);
        }
    }
}
