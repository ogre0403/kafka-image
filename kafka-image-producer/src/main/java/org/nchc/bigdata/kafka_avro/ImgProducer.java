package org.nchc.bigdata.kafka_avro;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.IOUtils;

import javax.imageio.ImageIO;


public class ImgProducer
{
    private static Producer<Integer, FileInputStream> producer;
    private final Properties props = new Properties();
    private String topic = "DUMMY_LOG";
    private ImageEncoder encoder;

    public ImgProducer(){
        props.put("metadata.broker.list", "master:9092,slave1:9092");
//      props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "org.nchc.bigdata.kafka_avro.ImageEncoder");
//        props.put("max.message.size","100000000");
        //props.put("request.required.acks", "1");
        producer = new Producer<Integer, FileInputStream>(new ProducerConfig(props));
        encoder = new ImageEncoder();
    }

    public static void main(String[] args) throws IOException {

        ImgProducer ap = new ImgProducer();
        File folder = new File("/D:/kafka_image");
        ap.sendImageInDir(folder);
        ap.close();

    }

    public void sendImageInDir(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                sendImageInDir(fileEntry);
            } else {
                sendImage(fileEntry.getCanonicalPath());
//                saveToDisk(fileEntry.getCanonicalPath());
            }
        }


    }

    private void sendImage(String imgFileName) throws IOException {

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

    public void close(){
        if (producer != null){
            producer.close();
        }
    }
}
