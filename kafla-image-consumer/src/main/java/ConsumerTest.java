import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.io.IOUtils;

import java.io.*;


public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        int i = 0;
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            i++;
            System.out.println("get image");

            try {
                FileOutputStream output = new FileOutputStream(new File("d:/result/"+i+".jpg"));
                IOUtils.write(it.next().message(), output);
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}