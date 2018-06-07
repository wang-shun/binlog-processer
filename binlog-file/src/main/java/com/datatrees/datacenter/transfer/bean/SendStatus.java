package com.datatrees.datacenter.transfer.bean;


/**
 * @author personalc
 */

public enum SendStatus {
    /**
     * 已经同步至t_binlog_process
     */
    YES(1),
    /**
     * 未同步至t_binlog_process
     */
    NO(0);

    private int value;

    SendStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
