package org.nchc.bigdata.kafka_avro;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ImgProducer
{
    private static Producer<Integer, String> producer;
    private final Properties props = new Properties();
    private String topic = "ogre.test";

    public ImgProducer(){
        props.put("metadata.broker.list", "master:9092,slave1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("request.required.acks", "1");
        producer = new Producer<Integer, String>(new ProducerConfig(props));
    }

    public static void main(String[] args) throws IOException {

        ImgProducer ap = new ImgProducer();
        ap.resource();
    }

    public void resource() throws IOException {
        URL resourceUrl = getClass().getResource("/DummyLog.avsc");
        Schema schema = Schema.parse(new File(resourceUrl.getPath()));

        GenericRecord user1 = null;
        int i=1000;
        while (i<1045) {
            user1 = new GenericData.Record(schema);
            user1.put("id", i);
            user1.put("logTime", 500+i);
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String> (topic, user1.toString());
            producer.send(data);
            i++;
        }
        producer.close();
    }

}
