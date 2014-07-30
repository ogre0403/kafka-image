package org.nchc.bigdata.kafka.producer;

import java.io.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.IOUtils;
import org.nchc.bigdata.kafka.encoder.ImageEncoder;


public class ImgProducer extends KafkaProducer
{
    public ImgProducer(){
        props.put("serializer.class", "org.nchc.bigdata.kafka.encoder.ImageEncoder");
        producer = new Producer<Integer, FileInputStream>(new ProducerConfig(props));
        encoder = new ImageEncoder();
    }

    protected void sendImage(String imgFileName) throws IOException {

        FileInputStream fis = new FileInputStream(imgFileName);
        KeyedMessage<Integer, FileInputStream> data =
                new KeyedMessage<Integer, FileInputStream> (topic, fis);
        producer.send(data);
        System.out.println(imgFileName);
    }

    private void saveToDisk(String imgFileName) throws IOException {
        FileInputStream imgPath = new FileInputStream(imgFileName);
        byte[] ba = encoder.toBytes(imgPath);
        FileOutputStream output = new FileOutputStream(new File("/D:/result/aaa.JPG"));
        IOUtils.write(ba, output);
        output.close();
    }


}
