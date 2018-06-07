package com.datatrees.datacenter.transfer.bean;


/**
 * @author personalc
 */

public enum HttpAccessStatus {
    /**
     * 文件大小未知道
     */
    FILE_SIZE_NOT_KNOWN(-1),
    /**
     * 文件不可访问
     */
    FILE_NOT_ACCESSIBLE(-2),
    /**
     * 连接不可用
     */
    HTTP_CONNECTION_RESPONSE_CODE(400);
    private int value;

    HttpAccessStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
