package org.nchc.bigdata.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.nchc.bigdata.SampleRun;
import org.nchc.bigdata.kafka.encoder.AvroEncoder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 1403035 on 2014/7/29.
 */
public class AvroProducer extends KafkaProducer{

    private Schema schema;
    public AvroProducer() throws IOException {
        super();
        props.put("serializer.class", "org.nchc.bigdata.kafka.encoder.AvroEncoder");
        File file3 = new File(SampleRun.class.getResource("/image.avsc").getFile());
        schema = Schema.parse(file3);
        producer = new Producer<Integer, GenericRecord>(new ProducerConfig(props));
        encoder = new AvroEncoder();
    }

    @Override
    protected void sendImage(String fname) throws IOException  {
        FileInputStream fis = new FileInputStream(fname);
        byte[] bbb = IOUtils.toByteArray(fis);
        ByteBuffer buf = ByteBuffer.wrap(bbb);
        GenericRecord datum = new GenericData.Record(schema);

        String[] resultTokens = fname.split("\\\\");
        datum.put("filename",resultTokens[resultTokens.length-1]);
        datum.put("raw",buf);

        KeyedMessage<Integer, GenericRecord> data =
                new KeyedMessage<Integer, GenericRecord> (topic, datum);
        producer.send(data);

        System.out.println(fname);
    }
}
