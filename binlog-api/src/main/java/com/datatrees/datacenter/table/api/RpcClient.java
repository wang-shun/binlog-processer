package com.datatrees.datacenter.table.api;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

public class RpcClient {

    private String host;
    private Integer port;
    private static final String CHARSET_NAME = "utf-8";
    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    private static Gson gson = new Gson();

    public RpcClient(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public SchemaResponse querySchema(SchemaRequest request, Integer timeOutSec) throws Exception {

        Socket socket = new Socket();
        InputStream inputStream = null;
        OutputStream outputStream = null;
        SchemaResponse rsp = null;

        try {

            byte[] content = gson.toJson(request).getBytes(CHARSET_NAME);
            byte[] meta = intToBytes(content.length);

            socket.connect(new InetSocketAddress(host, port), timeOutSec);

            outputStream = socket.getOutputStream();

            outputStream.write(mergeBytes(meta, content));
            outputStream.flush();

            inputStream = socket.getInputStream();
            byte[] b = new byte[1024];

            int n;
            byte[] result = null;
            while (-1 != (n = inputStream.read(b))) {
                result = mergeBytes(result, Arrays.copyOfRange(b, 0, n));
            }

            rsp = gson.fromJson(new String(result), SchemaResponse.class);

        } catch (Exception e) {
            //Unable to invoke no-args constructor for interface org.apache.kafka.connect.data.Schema. Register an InstanceCreator with Gson for this type may fix this problem.
            e.printStackTrace();
            logger.error("", e);
        } finally {
            if (null != inputStream) {
                inputStream.close();
            }
            if (null != outputStream) {
                outputStream.close();
            }
            if (!socket.isClosed()) {
                socket.close();
            }
        }
        return rsp;
    }


    public static byte[] intToBytes(int value) {
        byte[] result = new byte[4];
        // 由高位到低位
        result[0] = (byte) ((value >> 24) & 0xFF);
        result[1] = (byte) ((value >> 16) & 0xFF);
        result[2] = (byte) ((value >> 8) & 0xFF);
        result[3] = (byte) (value & 0xFF);
        return result;
    }

    public static byte[] mergeBytes(byte[] data1, byte[] data2) {
        if (data1 == null) {
            return data2;
        }
        if (data2 == null) {
            return data1;
        }
        byte[] data3 = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, data3, 0, data1.length);
        System.arraycopy(data2, 0, data3, data1.length, data2.length);
        return data3;

    }


}
