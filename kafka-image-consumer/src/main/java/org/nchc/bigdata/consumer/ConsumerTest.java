package org.nchc.bigdata.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.ByteBuffer;


public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private Configuration conf;
    private FileSystem fs;
    private DatumReader<GenericRecord> datumReader;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9000");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        File file3 = new File(ConsumerTest.class.getResource("/image.avsc").getFile());
        Schema schema = null;
        try {
            schema = Schema.parse(file3);
        } catch (IOException e) {
            e.printStackTrace();
        }
        datumReader = new GenericDatumReader<GenericRecord>(schema);

    }

    public void run() {
        int i = 0;

        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            i++;
            System.out.println("get image");

            try {

                // write to HDFS
                /*
                Path out = new Path("/out/result_"+i+".jpg");
                OutputStream os = fs.create(out);
                ByteArrayInputStream is = new ByteArrayInputStream(it.next().message());
                org.apache.hadoop.io.IOUtils.copyBytes(is, os, conf);
                */

                // write to local disk

                Decoder decoder = DecoderFactory.get().binaryDecoder(it.next().message(),null);
                GenericRecord rr = datumReader.read(null,decoder);
                FileOutputStream output = new FileOutputStream(new File("d:/result/"+rr.get("filename")));
                ByteBuffer bf = (ByteBuffer)rr.get("raw");
                byte[] ba = new byte[bf.capacity()];
                bf.get(ba,0,ba.length);
                IOUtils.write(ba, output);
                output.close();


            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}