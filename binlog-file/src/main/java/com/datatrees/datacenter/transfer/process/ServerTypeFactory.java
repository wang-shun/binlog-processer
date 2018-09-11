package com.datatrees.datacenter.transfer.process;


import com.datatrees.datacenter.transfer.process.local.LocalDataCenterTransfer;

/**
 * @author personalc
 */
public class ServerTypeFactory {
    public BinlogFileTransfer getServerType(String serverType) {
        if (serverType == null) {
            return null;
        }
        if (serverType.equalsIgnoreCase("AliBinLogFileTransfer")) {
            return new AliBinLogFileTransfer();
        }
        if (serverType.equalsIgnoreCase("LocalDataCenterTransfer")) {
            return new LocalDataCenterTransfer();
        }
        return null;
    }

}
