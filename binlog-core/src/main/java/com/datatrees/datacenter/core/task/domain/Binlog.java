package com.datatrees.datacenter.core.task.domain;

import java.io.Serializable;

public class Binlog implements Serializable{
    /**
     * 实例唯一编号 ,多个identity可以指向同一个instanceId, 一个instanceId包含多个database
     */
    String instanceId;
    /**
     * binlog 唯一编号
     */
    String identity;
    String path;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
