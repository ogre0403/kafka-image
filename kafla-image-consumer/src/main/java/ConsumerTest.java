import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


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

            BufferedImage img = null;
            try {
                img = ImageIO.read(new ByteArrayInputStream(it.next().message()));
                File outputfile = new File("d:/result/"+i+".jpg");
                ImageIO.write(img,"jpg",outputfile);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}