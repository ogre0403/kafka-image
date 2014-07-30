package org.nchc.bigdata;

import org.nchc.bigdata.kafka.producer.AvroProducer;
import org.nchc.bigdata.kafka.producer.KafkaProducer;

import java.io.*;

/**
 * Created by 1403035 on 2014/7/29.
 */
public class SampleRun {
    public static void main(String[] args) throws IOException {

        KafkaProducer pp = new AvroProducer();
        pp.sendImageInDir(new File("/D:/kafka_image"));
        pp.close();
    }
}
