package com.datatrees.datacenter.core.task.domain;

import java.io.Serializable;

public class Binlog implements Serializable {

    private String jdbcUrl;
    /**
     * binlog 唯一编号
     */
    private String identity;

    /**
     * 解析的binlog的完整路径
     */
    private String path;

    public Binlog(String path, String identity, String jdbcUrl) {
        this.identity = identity;
        this.jdbcUrl = jdbcUrl;
        this.identity = identity;
    }

    public String getInstanceId() {
        return jdbcUrl.substring(0, jdbcUrl.indexOf(".") - 1);
    }

    public String getIdentity() {
        return identity;
    }

    public String getPath() {
        return path;
    }

}
