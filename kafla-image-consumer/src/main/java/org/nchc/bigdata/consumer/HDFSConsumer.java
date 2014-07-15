package org.nchc.bigdata.consumer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.*;

/**
 * Created by 1403035 on 2014/7/14.
 */
public class HDFSConsumer {

    static NullWritable key = NullWritable.get();
    static BytesWritable value = new BytesWritable();

    public static void main(String[] args) throws IOException {
        System.out.println("HDFS TEST");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9000");

        saveToHDFS(conf);

    }

    private static void saveToHDFS(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("/out/aaa.jpg");
        OutputStream os = fs.create(out);
        InputStream is = new BufferedInputStream(new FileInputStream("/D:/kafka_image/IMG_0002.jpg"));
        IOUtils.copyBytes(is,os,conf);
    }

    private static void writeToSeqFile(Configuration conf) throws IOException {

        Path pp = new Path("/out/image.seq");
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer =
                SequenceFile.createWriter(fs, conf, pp,
                        key.getClass(), value.getClass());

        File localDir = new File("/D:/kafka_image");

        for (final File fileEntry : localDir.listFiles()) {
            append(writer, fileEntry);
        }



    }

    private static void append(SequenceFile.Writer writer, File fileEntry) throws IOException {
        FileInputStream fis = new FileInputStream(fileEntry.getCanonicalPath());
        byte[] ba = org.apache.commons.io.IOUtils.toByteArray(fis);
        value.set(ba,0,ba.length);
        writer.append(key,value);
    }
    private static void readFromSeqFile(){

    }
}
