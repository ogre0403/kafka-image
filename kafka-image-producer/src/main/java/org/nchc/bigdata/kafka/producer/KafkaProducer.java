package org.nchc.bigdata.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.serializer.Encoder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by 1403035 on 2014/7/30.
 */
public abstract class KafkaProducer {

    protected Properties props = new Properties();
    protected Producer producer;
    protected String topic = "binary.test";
    protected Encoder encoder;

    public KafkaProducer(){
        props.put("metadata.broker.list", "master:2092,slave1:2092");
//      props.put("serializer.class", "kafka.serializer.StringEncoder");

//        props.put("max.message.size","100000000");
        //props.put("request.required.acks", "1");
    }

    public void sendImageInDir(File folder) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                sendImageInDir(fileEntry);
            } else {
                sendImage(fileEntry.getCanonicalPath());
            }
        }
    }

    public void close(){
        if (producer != null){
            producer.close();
        }
    }
    protected abstract void sendImage(String fname) throws IOException;
}
