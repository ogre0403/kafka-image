package org.nchc.bigdata.kafka.encoder;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by 1403035 on 2014/7/11.
 */
public class ImageEncoder implements Encoder<FileInputStream> {

    private static Logger logger = Logger.getLogger(ImageEncoder.class);
    public ImageEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    public ImageEncoder(){}

    @Override
    public byte[] toBytes(FileInputStream fis) {
        try {
            return IOUtils.toByteArray(fis);
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return null;
    }
}
