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

import javax.imageio.ImageIO;


public class ImgProducer
{
    private static Producer<Integer, BufferedImage> producer;
    private final Properties props = new Properties();
    private String topic = "DUMMY_LOG";
    private ImageEncoder encoder;

    public ImgProducer(){
        props.put("metadata.broker.list", "master:9092,slave1:9092");
//      props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "org.nchc.bigdata.kafka_avro.ImageEncoder");
//        props.put("max.message.size","100000000");
        //props.put("request.required.acks", "1");
        producer = new Producer<Integer, BufferedImage>(new ProducerConfig(props));
        encoder = new ImageEncoder();
    }

    public static void main(String[] args) throws IOException {

        ImgProducer ap = new ImgProducer();
//        ap.resource();
        File folder = new File("/D:/big");
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

        File imgPath = new File(imgFileName);
        BufferedImage bufferedImage = ImageIO.read(imgPath);
        KeyedMessage<Integer, BufferedImage> data =
                new KeyedMessage<Integer, BufferedImage> (topic, bufferedImage);
        producer.send(data);
        System.out.println(imgFileName);
    }

    private void saveToDisk(String imgFileName) throws IOException {

        File imgPath = new File(imgFileName);
        BufferedImage bufferedImage = ImageIO.read(imgPath);
        byte[] ba = encoder.toBytes(bufferedImage);

        BufferedImage img = ImageIO.read(new ByteArrayInputStream(ba));
        File outputfile = new File("d:/result/aaa.jpg");
        ImageIO.write(img,"jpg",outputfile);

    }

    public void close(){
        if (producer != null){
            producer.close();
        }
    }
}
