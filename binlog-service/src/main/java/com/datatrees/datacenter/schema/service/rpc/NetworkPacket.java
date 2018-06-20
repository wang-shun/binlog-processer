package com.datatrees.datacenter.schema.service.rpc;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

public class NetworkPacket {

    private ByteBuffer size = ByteBuffer.allocate(4);

    private ByteBuffer content;

    private ByteBuffer rsp;

    private static Charset charset = Charset.forName("utf-8");

    public int read(SocketChannel socketChannel) throws IOException {
        int read = 0;
        if (size.hasRemaining()) {
            int bytesRead = socketChannel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new RuntimeException("Invalid receive (size = " + receiveSize + ")");
                this.content = ByteBuffer.allocate(receiveSize);
            }
        }
        if (content != null) {
            int bytesRead = socketChannel.read(content);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }
        return read;
    }

    public boolean complete() {
        return !size.hasRemaining() && !content.hasRemaining();
    }

    public String getContent() {
        content.flip();
        return charset.decode(content).toString();
    }

    public ByteBuffer getRsp() {
        return rsp;
    }

    public void setRsp(ByteBuffer rsp) {
        this.rsp = rsp;
    }
}
