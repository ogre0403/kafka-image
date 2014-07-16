package org.nchc.bigdata.consumer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

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

//        writeToSeqFile(conf);
        readFromSeqFile(conf);
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
        int i =0;
        for (final File fileEntry : localDir.listFiles()) {

            System.out.println(i++);
            append(writer, fileEntry);
        }
    }

    private static void append(SequenceFile.Writer writer, File fileEntry) throws IOException {
        FileInputStream fis = new FileInputStream(fileEntry.getCanonicalPath());
        byte[] ba = org.apache.commons.io.IOUtils.toByteArray(fis);
        System.out.println("sa length " + ba.length);
//        value = new BytesWritable(ba);
        value.set(ba,0,ba.length);
//        value.setSize(ba.length);
//        value.setCapacity(ba.length);
//        System.out.println("value length " + value.getBytes().length);
//        System.out.println("value buffer length " + value.getLength());
//        System.out.println("value capacity length " + value.getCapacity());
        writer.append(key,value);
    }
    private static void readFromSeqFile(Configuration conf) throws IOException {
        Path pp = new Path("/camus_bin/output/binary_test/hourly/2014/07/15/19/binary_test.1.1.4.3seq");
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader =  new SequenceFile.Reader(fs, pp,conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
        int i = 0;
        while(reader.next(key,value)){
            System.out.println(i++);
            int sss = ((BytesWritable) value).getLength();
            byte[] ba = new byte[sss];
            System.arraycopy(((BytesWritable) value).getBytes(),0,ba,0,sss);

            FileOutputStream output = new FileOutputStream(new File("d:/result/"+i+".jpg"));
            org.apache.commons.io.IOUtils.write(ba, output);
            output.close();
        }
    }
}
