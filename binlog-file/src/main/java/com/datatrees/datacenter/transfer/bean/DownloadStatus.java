package com.datatrees.datacenter.transfer.bean;

import java.io.Serializable;

/**
 * @author personalc
 */

public enum DownloadStatus implements Serializable {
    /**
     * 下载完成
     */
    COMPLETE(1),
    /**
     * 下载未完成
     */
    UNCOMPLETED(0);

    private int value;

    DownloadStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
