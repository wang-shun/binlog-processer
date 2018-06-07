package com.datatrees.datacenter.transfer.process;


/**
 * @author personalc
 */
public class ServerTypeFactory {
    public BinlogFileTransfer getServerType(String serverType)
    {
        if (serverType==null)
        {
            return null;
        }
        if (serverType.equalsIgnoreCase("AliBinLogFileTransfer")){
            return new AliBinLogFileTransfer();
        }
        return null;
    }

}
