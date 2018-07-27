package com.ijunhai.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.Document;
import org.jdom2.JDOMException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class HDFSUtil {
    private FileSystem fileSystem = null;

    /**
     * 读数据清洗的配置文件
     *
     * @param hdfs
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws JDOMException
     */
    public static Document readConfigFromHdfs(String hdfs) throws IOException, InterruptedException, JDOMException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");

        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] ioBuffer = new byte[1024 * 1024];
        int readLen = hdfsInStream.read(ioBuffer);

        while (-1 != readLen) {
            baos.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        Document doc = XMLUtil.xml2bson(baos);
        baos.close();
        hdfsInStream.close();
        fs.close();
        return doc;
    }

    /**
     * 读ip库
     *
     * @param hdfs
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws JDOMException
     * @throws IOException
     */
    public static byte[] readFromHdfs(String hdfs) throws InterruptedException, JDOMException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

        byte[] ioBuffer = new byte[hdfsInStream.available()];
        byte[] buf = new byte[1024 * 1024];
        int index = 0;
        int readLen = hdfsInStream.read(buf);
        while (-1 != readLen) {
            System.arraycopy(buf, 0, ioBuffer, index, readLen);
            index += readLen;
            readLen = hdfsInStream.read(buf);
        }
        hdfsInStream.close();
        fs.close();
        return ioBuffer;
    }


    /**
     * 上传文件到HDFS上去
     *
     * @param hdfs
     * @param in
     */
    public static void uploadToHdfs(String hdfs, byte[] in) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        int pos = 0;
        int batchSize = 4096;
        int currentSize = 0;
        FileSystem fs;
        OutputStream out;

        try {
            System.out.println("hdfs path :" + hdfs);
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            out = fs.create(new Path(hdfs));
        } catch (Exception e) {
            e.printStackTrace();
            hdfs = "hdfs://uhadoop-1neej2-master2/tmp";
            System.out.println("hdfs path :" + hdfs);
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            out = fs.create(new Path(hdfs));
        }

        while (pos < in.length) {
            currentSize = in.length - pos > batchSize ? batchSize : (in.length - pos);
            out.write(in, pos, currentSize);
            pos += currentSize;
        }
        out.close();
        fs.close();
    }
}
