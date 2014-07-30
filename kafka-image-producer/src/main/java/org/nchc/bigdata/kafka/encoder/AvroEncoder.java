package org.nchc.bigdata.kafka.encoder;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;
import org.nchc.bigdata.SampleRun;

import java.io.*;


/**
 * Created by 1403035 on 2014/7/30.
 */
public class AvroEncoder implements Encoder<GenericRecord> {

    private static Logger logger = Logger.getLogger(AvroEncoder.class);
    public AvroEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    public AvroEncoder(){}

    @Override
    public byte[] toBytes(GenericRecord datum) {
        File file3 = new File(SampleRun.class.getResource("/image.avsc").getFile());
        Schema schema = null;
        try {
            schema = Schema.parse(file3);
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        try {
            writer.write(datum, encoder);
            encoder.flush();
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return out.toByteArray();
    }
}
