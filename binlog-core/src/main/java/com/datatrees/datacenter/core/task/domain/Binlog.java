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

    public Binlog() {
    }

    public Binlog(String path, String identity, String jdbcUrl) {
        this.path = path;
        this.jdbcUrl = jdbcUrl;
        this.identity = identity;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getInstanceId() {
        return jdbcUrl.substring(0, jdbcUrl.indexOf("."));
    }

    public String getIdentity() {
        return identity;
    }

    public String getIdentity1() {
        return getIdentity().replace(".tar", "");
    }

    public String getIdentity0() {
        return getIdentity().split("_")[0];
    }

    public String getPath() {
        return path;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[" + this.identity + ",");
        stringBuilder.append("[" + this.jdbcUrl + ",");
        stringBuilder.append(this.path + "]");
        return stringBuilder.toString();
    }
}
