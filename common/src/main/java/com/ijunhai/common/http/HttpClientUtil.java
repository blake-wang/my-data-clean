package com.ijunhai.common.http;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;
import java.io.InputStream;

public class HttpClientUtil {
    HttpClient httpClient = null;
    private int connectTimeOut = 5000;
    private int socketTimeOut = 10000;

    public HttpClientUtil() {
        this.httpClient = new HttpClient();
    }

    public Result doGet(String url) {
        Result result = new Result();
        int batchSize = 102400;
        HttpMethod method = new GetMethod(url);
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(connectTimeOut);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(socketTimeOut);

        try {
            int statusCode = httpClient.executeMethod(method);
            InputStream in = method.getResponseBodyAsStream();
            StringBuilder ioBuffer = new StringBuilder("");
            byte[] buf = new byte[batchSize];
            int readLen = in.read(buf);
            while (-1 != readLen) {
                ioBuffer.append(new String(buf), 0, readLen);
                readLen = in.read(buf);
            }
            String content = ioBuffer.toString();
            result.setStatusCode(statusCode);
            result.setContent(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
