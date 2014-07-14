package org.nchc.bigdata.kafka_avro;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.nio.ByteBuffer;

/**
 * Created by 1403035 on 2014/7/11.
 */
public class CustomEncoder implements Encoder<Long>{

    public CustomEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }


    @Override
    public byte[] toBytes(Long aLong) {
        byte[] bytes = ByteBuffer.allocate(8).putLong(aLong).array();
        return bytes;
    }
}
