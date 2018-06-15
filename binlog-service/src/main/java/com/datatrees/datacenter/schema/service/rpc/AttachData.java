package com.datatrees.datacenter.schema.service.rpc;

import java.nio.ByteBuffer;

public class AttachData {

    private int readSize;

    private ByteBuffer readBuffer;

    private ByteBuffer writeBuffer;

    public AttachData(int contentLength, ByteBuffer readBuffer) {
        this.readSize = contentLength;
        this.readBuffer = readBuffer;
    }

    public AttachData(){

    }

    public int getSize() {
        return readSize;
    }

    public void setSize(int size) {
        this.readSize = size;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public void setReadBuffer(ByteBuffer readBuffer) {
        this.readBuffer = readBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public void setWriteBuffer(ByteBuffer writeBuffer) {
        this.writeBuffer = writeBuffer;
    }
}
