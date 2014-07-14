package org.nchc.bigdata.kafka_avro;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by 1403035 on 2014/7/11.
 */
public class ImageEncoder implements Encoder<BufferedImage> {

    public ImageEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    public ImageEncoder(){}

    @Override
    public byte[] toBytes(BufferedImage bufferedImage) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ImageIO.write(bufferedImage, "jpg", baos);
            baos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            return null;
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        /*
        WritableRaster raster = bufferedImage .getRaster();
        DataBufferByte data   = (DataBufferByte) raster.getDataBuffer();
        return ( data.getData() );
        */
    }
}
