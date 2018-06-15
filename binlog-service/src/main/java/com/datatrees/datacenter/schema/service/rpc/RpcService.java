package com.datatrees.datacenter.schema.service.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class RpcService {

    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private static Logger logger = LoggerFactory.getLogger(RpcService.class);
    private Thread thread;


    public RpcService(){

    }

    public void init() throws Exception{
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", 8801));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        thread=new Thread(this::execute, "RpcService");
    }

    public void start(){
        thread.start();
        logger.info("started rpc service");
    }

    public void execute(){
        while (true){
            try {
                int readyCount = selector.select();
                if (readyCount == 0)
                    break;

                Iterator<SelectionKey> selected = selector.selectedKeys().iterator();

                while (selected.hasNext()){

                    SelectionKey selectionKey = selected.next();
                    selected.remove();

                    //链接上后注册写和读事件
                    if (selectionKey.isAcceptable()){
                        ServerSocketChannel serverChannel = (ServerSocketChannel) selectionKey.channel();
                        SocketChannel socketChannel = serverChannel.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector, SelectionKey.OP_READ, new NetworkPacket());
                    }

                    if (selectionKey.isReadable()){
                        new RpcHandler().handleRead(selectionKey);
                    }

                    if (selectionKey.isWritable()){
                        new RpcHandler().handleWrite(selectionKey);
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws Exception{
        RpcService service = new RpcService();
        service.init();
        service.start();
    }

}
