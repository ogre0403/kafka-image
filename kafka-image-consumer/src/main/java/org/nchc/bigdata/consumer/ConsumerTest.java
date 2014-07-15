package org.nchc.bigdata.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;


public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private Configuration conf;
    private FileSystem fs;

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
    }

    public void run() {
        int i = 0;
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            i++;
            System.out.println("get image");

            try {

                Path out = new Path("/out/result_"+i+".jpg");
                OutputStream os = fs.create(out);
                ByteArrayInputStream is = new ByteArrayInputStream(it.next().message());
                org.apache.hadoop.io.IOUtils.copyBytes(is, os, conf);

                /*
                FileOutputStream output = new FileOutputStream(new File("d:/result/"+i+".jpg"));
                IOUtils.write(it.next().message(), output);
                output.close();
                */
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}