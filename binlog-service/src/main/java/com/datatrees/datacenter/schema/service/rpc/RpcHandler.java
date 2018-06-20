package com.datatrees.datacenter.schema.service.rpc;

import com.datatrees.datacenter.schema.api.ResultCode;
import com.datatrees.datacenter.schema.api.SchemaRequest;
import com.datatrees.datacenter.schema.api.SchemaResponse;
import com.datatrees.datacenter.schema.service.repository.SchemaRepository;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class RpcHandler {

    private Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    private static final Gson gson = new Gson();

    public void handleRead(SelectionKey key) {

        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            NetworkPacket readPacket = (NetworkPacket) key.attachment();

            readPacket.read(socketChannel);
            if (readPacket.complete()){
                processAndResponse(key);
            }

        } catch (Throwable t) {
            logger.error("failed to process request", t);
            handleError(key);
        }
    }

    private void handleError(SelectionKey key) {

        NetworkPacket packet = (NetworkPacket) key.attachment();

        SchemaResponse schemaResponse = new SchemaResponse();
        schemaResponse.setResultCode(ResultCode.FAIL);
        schemaResponse.setSchema("");
        String rsp = gson.toJson(schemaResponse);

        ByteBuffer buffer = ByteBuffer.allocate(rsp.getBytes().length);
        buffer.put(rsp.getBytes());
        packet.setRsp(buffer);
        //cancel read and interest in write
        key.interestOps(SelectionKey.OP_WRITE);
    }

    private void processAndResponse(SelectionKey key) throws Exception {

        NetworkPacket packet = (NetworkPacket) key.attachment();

        SchemaRequest schemaRequest = gson.fromJson(packet.getContent(), SchemaRequest.class);

        String schema = SchemaRepository.queryAvroSchema(schemaRequest);

        SchemaResponse schemaResponse = new SchemaResponse();
        if (null == schema){
            schemaResponse.setResultCode(ResultCode.NOT_EXIST);
        } else {
            schemaResponse.setResultCode(ResultCode.SUCCESS);
        }
        schemaResponse.setSchema(schema);

        String rsp = gson.toJson(schemaResponse);

        ByteBuffer buffer = ByteBuffer.allocate(rsp.getBytes().length);
        buffer.put(rsp.getBytes());
        buffer.flip();
        packet.setRsp(buffer);
        //cancel read and interest in write
        key.interestOps(SelectionKey.OP_WRITE);

    }

    public void handleWrite(SelectionKey key) {
        try {

            NetworkPacket data = (NetworkPacket) key.attachment();
            SocketChannel channel = (SocketChannel) key.channel();
            if (data == null || data.getRsp() == null) {
                key.cancel();
                channel.close();
                return;
            }
            ByteBuffer writeBuffer = data.getRsp();
            channel.write(writeBuffer);
            if (!writeBuffer.hasRemaining()) {
                logger.info("processed request: {}", channel.getRemoteAddress());
                key.cancel();
                channel.close();
            }
        } catch (Throwable t) {
            logger.error("failed to handle request");
            key.cancel();
        }

    }

}
